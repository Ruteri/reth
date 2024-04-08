use crate::{
    table::{Compress, DupSort, Encode, KeyFormat, Table, TableImporter},
    tables::utils::decode_one,
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};

use std::sync::Mutex;

use crate::reth_rocksdb;
use crate::reth_rocksdb::cursor::Cursor;
use reth_interfaces::db::{DatabaseErrorInfo, DatabaseWriteError, DatabaseWriteOperation};

use std::fmt;

use rocksdb;

pub struct Tx<'db, DB> {
    pub inner: Mutex<Option<rocksdb::Transaction<'db, DB>>>,
    // TODO: cache column families instead of getting them each time
    pub db: &'db DB,
}

impl fmt::Debug for Tx<'_, rocksdb::TransactionDB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tx").finish()
    }
}

impl<'db> Tx<'db, rocksdb::TransactionDB> {
    // TODO: cf handles
    pub fn new(
        inner: rocksdb::Transaction<'db, rocksdb::TransactionDB>,
        db: &'db rocksdb::TransactionDB,
    ) -> Self {
        Self { inner: Mutex::new(Some(inner)), db }
    }
}

impl<'db> DbTx for Tx<'db, rocksdb::TransactionDB> {
    type Cursor<T: Table> = Cursor<'db, 'db, T>;
    type DupCursor<T: DupSort> = Cursor<'db, 'db, T>;

    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError> {
        let locked_inner = self.inner.lock().unwrap();
        if T::TABLE.is_dupsort() {
            let cf_handle = self.db.cf_handle(&String::from(T::NAME)).unwrap();
            let mut it = locked_inner.as_ref().unwrap().raw_iterator_cf(cf_handle);
            it.seek(key.encode());
            return match it.value() {
                None => Ok(None),
                Some(v) => reth_rocksdb::cursor::decode_value::<T>(v),
            };
        } else {
            locked_inner
                .as_ref()
                .unwrap()
                .get_cf(self.db.cf_handle(&String::from(T::NAME)).unwrap(), key.encode())
                .map_err(|e| {
                    DatabaseError::Read(DatabaseErrorInfo { message: e.to_string(), code: 1 })
                })?
                .map(|data| decode_one::<T>(data.into()))
                .transpose()
        }
    }

    fn commit(mut self) -> Result<bool, DatabaseError> {
        let moved_out_tx = self.inner.get_mut().unwrap().take().unwrap();
        moved_out_tx.commit().map_err(|e| {
            DatabaseError::Commit(DatabaseErrorInfo { message: e.to_string(), code: 1 })
        })?;
        Ok(true)
    }

    fn abort(self) {}

    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError> {
        // Most likely incorrect. To be fixed!
        // Cursor actually implements Sync trait, so I'm not sure if it's okay to get the raw
        // reference to Transaction and leak it outisde the Mutex -- probably not and there's
        // probably a better way to do it.
        let locked_opt_tx = self.inner.lock().unwrap();
        let tx_ref = locked_opt_tx.as_ref().unwrap();
        let cf_handle = self.db.cf_handle(&String::from(T::NAME)).unwrap();

        let raw_tx_ptr = tx_ref as *const rocksdb::Transaction<'db, rocksdb::TransactionDB>;
        let raw_self_ptr = self as *const Self;

        unsafe {
            let escaping_tx_ref: &rocksdb::Transaction<'db, rocksdb::TransactionDB> = &*raw_tx_ptr;
            Ok(Cursor::new(escaping_tx_ref.raw_iterator_cf(cf_handle), &*raw_self_ptr))
        }
    }

    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError> {
        let locked_opt_tx = self.inner.lock().unwrap();
        let tx_ref = locked_opt_tx.as_ref().unwrap();
        let cf_handle = self.db.cf_handle(&String::from(T::NAME)).unwrap();

        let raw_tx_ptr = tx_ref as *const rocksdb::Transaction<'db, rocksdb::TransactionDB>;
        let raw_self_ptr = self as *const Self;

        let mut opts = rocksdb::ReadOptions::default();
        opts.set_total_order_seek(true);

        unsafe {
            let escaping_tx_ref: &rocksdb::Transaction<'db, rocksdb::TransactionDB> = &*raw_tx_ptr;
            Ok(Cursor::new(escaping_tx_ref.raw_iterator_cf_opt(cf_handle, opts), &*raw_self_ptr))
        }
    }

    fn entries<T: Table>(&self) -> Result<usize, DatabaseError> {
        Ok(0)
    }

    fn disable_long_read_transaction_safety(&mut self) {}
}

impl<'db> DbTxMut for Tx<'db, rocksdb::TransactionDB> {
    type CursorMut<T: Table> = Cursor<'db, 'db, T>;
    type DupCursorMut<T: DupSort> = Cursor<'db, 'db, T>;

    fn put<T: Table>(&self, _key: T::Key, _value: T::Value) -> Result<(), DatabaseError> {
        let key = <T as KeyFormat<T::Key, T::Value>>::format_key(_key, &_value);
        let value = _value.compress();

        let cf_handle = self.db.cf_handle(&String::from(T::NAME)).unwrap();

        self.inner.lock().unwrap().as_mut().unwrap().put_cf(cf_handle, &key, value).map_err(|e| {
            DatabaseWriteError {
                info: DatabaseErrorInfo { message: e.to_string(), code: 1 },
                operation: DatabaseWriteOperation::Put,
                table_name: T::NAME,
                key: key.into(),
            }
            .into()
        })
    }

    fn delete<T: Table>(
        &self,
        _key: T::Key,
        _value: Option<T::Value>,
    ) -> Result<bool, DatabaseError> {
        let locked_opt_tx = self.inner.lock().unwrap();
        let tx = locked_opt_tx.as_ref().unwrap();
        let cf_handle = self.db.cf_handle(&String::from(T::NAME)).unwrap();

        let _ = match _value {
            None => tx.delete_cf(cf_handle, _key.encode().as_ref()),
            Some(value) => tx.delete_cf(cf_handle, &T::format_key(_key, &value)),
        };
        Ok(true)
    }

    fn clear<T: Table>(&self) -> Result<(), DatabaseError> {
        /*
        self.db.drop_cf(T::NAME).map_err(|e| {
            DatabaseError::Delete(DatabaseErrorInfo { message: e.to_string(), code: 1 })
        })?;
        self.db.create_cf(T::NAME, &rocksdb::Options::default()).map_err(|e| {
            DatabaseError::CreateTable(DatabaseErrorInfo { message: e.to_string(), code: 1 })
        })
        */

        /* This is extremely inefficient, workaround for the db not being mutable */
        let locked_opt_tx = self.inner.lock().unwrap();
        let tx = locked_opt_tx.as_ref().unwrap();
        let cf_handle = self.db.cf_handle(&String::from(T::NAME)).unwrap();
        let mut it = tx.raw_iterator_cf(cf_handle);
        it.seek_to_first();
        while let Some(key) = it.key() {
            let _ = tx.delete_cf(cf_handle, key);
            it.next();
        }
        Ok(())
    }

    fn cursor_write<T: Table>(&self) -> Result<Self::CursorMut<T>, DatabaseError> {
        let locked_opt_tx = self.inner.lock().unwrap();
        let tx_ref = locked_opt_tx.as_ref().unwrap();
        let cf_handle = self.db.cf_handle(&String::from(T::NAME)).unwrap();

        let raw_tx_ptr = tx_ref as *const rocksdb::Transaction<'db, rocksdb::TransactionDB>;
        let raw_self_ptr = self as *const Self;

        unsafe {
            let escaping_tx_ref: &rocksdb::Transaction<'db, rocksdb::TransactionDB> = &*raw_tx_ptr;
            Ok(Cursor::new(escaping_tx_ref.raw_iterator_cf(cf_handle), &*raw_self_ptr))
        }
    }

    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError> {
        let locked_opt_tx = self.inner.lock().unwrap();
        let tx_ref = locked_opt_tx.as_ref().unwrap();
        let cf_handle = self.db.cf_handle(&String::from(T::NAME)).unwrap();

        let raw_tx_ptr = tx_ref as *const rocksdb::Transaction<'db, rocksdb::TransactionDB>;
        let raw_self_ptr = self as *const Self;

        let mut opts = rocksdb::ReadOptions::default();
        opts.set_total_order_seek(true);

        unsafe {
            let escaping_tx_ref: &rocksdb::Transaction<'db, rocksdb::TransactionDB> = &*raw_tx_ptr;
            Ok(Cursor::new(escaping_tx_ref.raw_iterator_cf_opt(cf_handle, opts), &*raw_self_ptr))
        }
    }
}

impl TableImporter for Tx<'_, rocksdb::TransactionDB> {}
