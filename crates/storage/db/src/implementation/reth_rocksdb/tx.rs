use crate::{
    cursor::DbCursorRW,
    table::{Compress, DupSort, Encode, KeyFormat, Table, TableImporter},
    tables::utils::decode_one,
    transaction::{DbTx, DbTxMut},
    unformat_extended_composite_key, DatabaseError,
};

use num_bigint;
use num_traits;
use std::sync::Mutex;

use crate::reth_rocksdb;
use crate::reth_rocksdb::cursor::Cursor;
use reth_interfaces::db::{DatabaseErrorInfo, DatabaseWriteError, DatabaseWriteOperation};

use std::fmt;

use rocksdb;

pub struct Tx<'db, DB> {
    pub inner: Mutex<Option<rocksdb::Transaction<'db, DB>>>,
    pub db: &'db DB,
}

impl fmt::Debug for Tx<'_, rocksdb::TransactionDB> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Tx").finish()
    }
}

impl<'db> Tx<'db, rocksdb::TransactionDB> {
    pub fn new(
        inner: rocksdb::Transaction<'db, rocksdb::TransactionDB>,
        db: &'db rocksdb::TransactionDB,
    ) -> Self {
        Self { inner: Mutex::new(Some(inner)), db }
    }

    pub fn get_with_value<T: Table>(
        &self,
        key: T::Key,
        value: &T::Value,
    ) -> Result<Option<T::Value>, DatabaseError> {
        let cf_handle = self.db.cf_handle(&String::from(T::NAME)).unwrap();
        let ext_key = T::format_key(key.clone(), value);

        let locked_inner = self.inner.lock().unwrap();
        let mut it = locked_inner.as_ref().unwrap().raw_iterator_cf(cf_handle);
        it.seek(&ext_key);
        return match it.item() {
            None => Ok(None),
            Some(el) => {
                if ext_key == el.0 {
                    reth_rocksdb::cursor::decode_value::<T>(el.1)
                } else {
                    Ok(None)
                }
            }
        };
    }

    pub fn put_raw<T: Table>(&self, _key: Vec<u8>, _value: Vec<u8>) -> Result<(), DatabaseError> {
        // println!("putting {:?}.{:02x?} {:02x?}", T::NAME, &_key, &_value);

        let cf_handle = self.db.cf_handle(&String::from(T::NAME)).unwrap();

        self.inner.lock().unwrap().as_mut().unwrap().put_cf(cf_handle, &_key, _value).map_err(|e| {
            DatabaseWriteError {
                info: DatabaseErrorInfo { message: e.to_string(), code: 1 },
                operation: DatabaseWriteOperation::Put,
                table_name: T::NAME,
                key: _key.into(),
            }
            .into()
        })
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
            let encoded_key = key.clone().encode();
            it.seek(&encoded_key);
            return match it.item() {
                None => Ok(None),
                Some(el) => {
                    if key == T::unformat_key(el.0) {
                        reth_rocksdb::cursor::decode_value::<T>(el.1)
                    } else {
                        Ok(None)
                    }
                }
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
        let cf_handle = self.db.cf_handle(&String::from(T::NAME)).unwrap();

        let locked_opt_tx = self.inner.lock().unwrap();
        let tx_ref = locked_opt_tx.as_ref().unwrap();

        let opts = rocksdb::ReadOptions::default();
        let mut it = tx_ref.raw_iterator_cf_opt(cf_handle, opts);
        it.seek_to_last();
        if !it.valid() {
            return Ok(0);
        }

        let last_key_as_bigint =
            num_bigint::BigInt::from_bytes_be(num_bigint::Sign::Plus, it.key().unwrap());

        it.seek_to_first();
        let first_el = it.item().unwrap();
        let first_key_as_bigint =
            num_bigint::BigInt::from_bytes_be(num_bigint::Sign::Plus, first_el.0);

        for i in 1..1000 {
            it.next();
            if !it.valid() {
                return Ok(i);
            }
        }

        let twentieth_el = it.item().unwrap();
        let twentieth_key_as_bigint =
            num_bigint::BigInt::from_bytes_be(num_bigint::Sign::Plus, twentieth_el.0);

        // Maybe better estimation: see how many times you can recursively halve the distance
        let first_to_last = last_key_as_bigint - first_key_as_bigint.clone();
        let first_to_twentieth = twentieth_key_as_bigint - first_key_as_bigint;
        let est_diff = first_to_last / first_to_twentieth;
        match num_traits::ToPrimitive::to_u64(&est_diff) {
            None => Ok(usize::MAX),
            Some(diff) => match (1000 * diff).try_into() {
                Err(_) => Ok(usize::MAX),
                Ok(diff_usize) => Ok(diff_usize),
            },
        }
    }

    fn disable_long_read_transaction_safety(&mut self) {}
}

impl<'db> DbTxMut for Tx<'db, rocksdb::TransactionDB> {
    type CursorMut<T: Table> = Cursor<'db, 'db, T>;
    type DupCursorMut<T: DupSort> = Cursor<'db, 'db, T>;

    fn put<T: Table>(&self, _key: T::Key, _value: T::Value) -> Result<(), DatabaseError> {
        self.cursor_write::<T>()?.upsert(_key, _value)
    }

    fn delete<T: Table>(
        &self,
        key: T::Key,
        _value: Option<T::Value>,
    ) -> Result<bool, DatabaseError> {
        // println!("deleting {:?}.{:02x?} {:02x?}", T::NAME, &key, &_value);
        let locked_opt_tx = self.inner.lock().unwrap();
        let tx = locked_opt_tx.as_ref().unwrap();
        let cf_handle = self.db.cf_handle(&String::from(T::NAME)).unwrap();

        let mut it = tx.raw_iterator_cf(cf_handle);

        if !T::TABLE.is_dupsort() {
            let key_to_seek = key.encode().as_ref().to_vec();

            it.seek(&key_to_seek);

            match it.item().filter(|el| el.0 == key_to_seek) {
                None => Ok(false),
                Some(el) => {
                    let _ = tx.delete_cf(cf_handle, el.0);
                    Ok(true)
                }
            }
        } else {
            let value = _value.expect("value not set for dupsort delete");
            let composite_key = T::format_key(key, &value);
            it.seek(&composite_key);

            let value = value.compress();

            while let Some(el) = it
                .item()
                .filter(|el| unformat_extended_composite_key::<T>(el.0.to_vec()) == composite_key)
            {
                if el.1 == value.as_ref() {
                    let _ = tx.delete_cf(cf_handle, el.0);
                    return Ok(true);
                }
                it.next();
            }
            Ok(false)
        }
    }

    fn clear<T: Table>(&self) -> Result<(), DatabaseError> {
        /* TODO: This is extremely inefficient, workaround for the db not being mutable
        self.db.drop_cf(T::NAME).map_err(|e| {
            DatabaseError::Delete(DatabaseErrorInfo { message: e.to_string(), code: 1 })
        })?;
        self.db.create_cf(T::NAME, &rocksdb::Options::default()).map_err(|e| {
            DatabaseError::CreateTable(DatabaseErrorInfo { message: e.to_string(), code: 1 })
        })
        */

        let locked_opt_tx = self.inner.lock().unwrap();
        let tx = locked_opt_tx.as_ref().unwrap();
        let cf_handle = self.db.cf_handle(&String::from(T::NAME)).unwrap();
        let mut it = tx.raw_iterator_cf(cf_handle);
        it.seek_to_first();
        while let Some(key) = it.key() {
            let _ = tx.delete_cf(cf_handle, key);
            it.seek_to_first();
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
