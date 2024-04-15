use crate::{
    common::{IterPairResult, PairResult, ValueOnlyResult},
    cursor::{
        self, DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW, DupWalker, RangeWalker,
        ReverseWalker, Walker,
    },
    max_extend_composite_key, reth_rocksdb,
    table::{Compress, Decode, Decompress, DupSort, Encode, KeyFormat, Table},
    tables::utils::{decode_one, decoder},
    transaction::{DbTx, DbTxMut},
    unformat_extended_composite_key, up_extend_composite_key, zero_extend_composite_key,
    DatabaseError,
};

use core::ops::Bound;
use core::ops::Deref;
use reth_interfaces::db::DatabaseErrorInfo;
use reth_interfaces::db::{DatabaseWriteError, DatabaseWriteOperation};
use reth_primitives::ForkId;

use std::ops::RangeBounds;
use std::{borrow::BorrowMut, fmt};
use std::{borrow::Cow, iter::Rev};

use rocksdb;

use rocksdb::Direction::{self, Forward, Reverse};

#[derive(Debug, Clone)]
enum CursorIt {
    Start,
    End, // Past last element
    Iterating,
}

/// Cursor that iterates over table
pub struct Cursor<'itx, 'it, T: Table> {
    pub iter: rocksdb::DBRawIteratorWithThreadMode<
        'it,
        rocksdb::Transaction<'it, rocksdb::TransactionDB>,
    >,
    pub tx: &'itx reth_rocksdb::tx::Tx<'it, rocksdb::TransactionDB>,
    pub state: CursorIt,
    pub dup_mode: bool,
    table_type: std::marker::PhantomData<T>,
}

impl<'itx, 'it: 'itx, T: Table> Cursor<'itx, 'it, T> {
    pub fn new(
        mut iter: rocksdb::DBRawIteratorWithThreadMode<
            'it,
            rocksdb::Transaction<'_, rocksdb::TransactionDB>,
        >,
        tx: &'itx reth_rocksdb::tx::Tx<'it, rocksdb::TransactionDB>,
        dup_mode: bool,
    ) -> Cursor<'itx, 'it, T> {
        Self { iter, tx, state: CursorIt::Start, dup_mode, table_type: std::marker::PhantomData }
    }
}

impl<T: Table> fmt::Debug for Cursor<'_, '_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cursor").finish()
    }
}

impl<T: Table> DbCursorRO<T> for Cursor<'_, '_, T> {
    fn first(&mut self) -> PairResult<T> {
        self.iter.seek_to_first();
        match self.iter.item() {
            None => {
                self.state = CursorIt::End;
                Ok(None)
            }
            Some(el) => {
                self.state = CursorIt::Iterating;
                decode_item::<T>(Some(el))
            }
        }
    }

    fn seek_exact(&mut self, _key: T::Key) -> PairResult<T> {
        Ok(self.seek(_key.clone())?.filter(|el| el.0 == _key))
    }

    fn seek(&mut self, _key: T::Key) -> PairResult<T> {
        let encoded_key = _key.clone().encode();
        self.iter.seek(encoded_key.as_ref());
        match self.iter.item() {
            None => {
                self.state = CursorIt::End;
                Ok(None)
            }
            Some(el) => {
                self.state = CursorIt::Iterating;
                decode_item::<T>(Some(el))
            }
        }
    }

    fn next(&mut self) -> PairResult<T> {
        match self.state {
            CursorIt::Start => self.first(),
            CursorIt::End => match self.iter.item() {
                None => Ok(None),
                Some(el) => match self.iter.item() {
                    None => Ok(None),
                    Some(_) => {
                        self.iter.next();
                        match decode_item::<T>(self.iter.item())? {
                            None => Ok(None),
                            Some(el) => {
                                self.state = CursorIt::Iterating;
                                Ok(Some(el))
                            }
                        }
                    }
                },
            },
            CursorIt::Iterating => {
                self.iter.next();
                match self.iter.item() {
                    None => {
                        self.iter.seek_to_last();
                        self.state = CursorIt::Iterating;
                        Ok(None)
                    }
                    Some(el) => {
                        self.state = CursorIt::Iterating;
                        decode_item::<T>(Some(el))
                    }
                }
            }
        }
    }

    fn prev(&mut self) -> PairResult<T> {
        match self.state {
            CursorIt::Start => self.last(),
            CursorIt::End => {
                // TODO: handle the case where end points at a midpoint
                match self.iter.item() {
                    None => {
                        self.iter.seek_to_last();
                        self.state = CursorIt::Iterating;
                        self.current()
                    }
                    Some(_) => {
                        self.iter.prev();
                        self.state = CursorIt::Iterating;
                        match self.current()? {
                            None => {
                                self.state = CursorIt::Start;
                                Ok(None)
                            }
                            Some(el) => Ok(Some(el)),
                        }
                    }
                }
            }
            CursorIt::Iterating => {
                self.iter.prev();
                match self.iter.item() {
                    None => {
                        self.state = CursorIt::Iterating;
                        self.iter.seek_to_first();
                        Ok(None)
                    }
                    Some(el) => {
                        self.state = CursorIt::Iterating;
                        decode_item::<T>(Some(el))
                    }
                }
            }
        }
    }

    fn last(&mut self) -> PairResult<T> {
        self.iter.seek_to_last();
        match self.iter.item() {
            None => {
                // This should probably Iterating with a None item
                self.state = CursorIt::End;
                Ok(None)
            }
            Some(el) => {
                self.state = CursorIt::Iterating;
                decode_item::<T>(Some(el))
            }
        }
    }

    fn current(&mut self) -> PairResult<T> {
        match self.state {
            CursorIt::Start => Ok(None),
            CursorIt::End => Ok(None),
            CursorIt::Iterating => decode_item::<T>(self.iter.item()),
        }
    }

    fn walk(&mut self, start_key: Option<T::Key>) -> Result<Walker<'_, T, Self>, DatabaseError> {
        let start: IterPairResult<T> = match start_key {
            Some(key) => self.seek(key).transpose(),
            None => self.first().transpose(),
        };
        Ok(Walker::new(self, start))
    }

    fn walk_range(
        &mut self,
        range: impl RangeBounds<T::Key>,
    ) -> Result<RangeWalker<'_, T, Self>, DatabaseError> {
        let start_key = match range.start_bound() {
            Bound::Included(key) | Bound::Excluded(key) => Some((*key).clone()),
            Bound::Unbounded => None,
        };

        let start_item = match start_key {
            None => self.first().transpose(),
            Some(key) => self.seek(key).transpose(),
        };

        Ok(RangeWalker::new(self, start_item, range.end_bound().cloned()))
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError> {
        let start: IterPairResult<T> = match start_key {
            None => self.last().transpose(),
            Some(key) => self.seek(key).transpose(),
        };
        Ok(ReverseWalker::new(self, start))
    }
}

impl<T: DupSort> DbDupCursorRO<T> for Cursor<'_, '_, T> {
    fn next_dup(&mut self) -> PairResult<T> {
        match self.state {
            CursorIt::Start => self.first(),
            CursorIt::End => Ok(None),
            CursorIt::Iterating => match self.iter.item() {
                None => self.next(),
                Some(prev_item) => {
                    let prev_primary = T::unformat_key(prev_item.0.to_vec());
                    match self.next()? {
                        None => Ok(None),
                        Some(el) => {
                            if prev_primary == el.0 {
                                Ok(Some(el))
                            } else {
                                self.prev()?;
                                Ok(None)
                            }
                        }
                    }
                }
            },
        }
    }

    fn next_no_dup(&mut self) -> PairResult<T> {
        match self.state {
            CursorIt::Start => self.first(),
            CursorIt::End => Ok(None),
            CursorIt::Iterating => {
                let prev_item = self.iter.item();
                if prev_item.is_none() {
                    return self.next();
                }

                let prev_item_key = prev_item.unwrap().0.to_vec();

                let mut prev_primary_plus_one: Vec<u8> =
                    T::unformat_key(prev_item_key.clone()).encode().into();
                for i in (1..prev_primary_plus_one.len()).rev() {
                    if prev_primary_plus_one[i] != u8::max_value() {
                        prev_primary_plus_one[i] = prev_primary_plus_one[i] + 1;
                        break;
                    } else {
                        prev_primary_plus_one[i] = 0;
                    }
                }

                self.iter.seek(prev_primary_plus_one.to_vec());
                match self.iter.item() {
                    None => {
                        self.iter.seek(&prev_item_key);
                        self.state = CursorIt::Iterating;
                        Ok(None)
                    }
                    Some(el) => {
                        self.state = CursorIt::Iterating;
                        decode_item::<T>(Some(el))
                    }
                }
            }
        }
    }

    fn next_dup_val(&mut self) -> ValueOnlyResult<T> {
        Ok(self.next_dup()?.map(|el| el.1.into()))
    }

    fn seek_by_key_subkey(
        &mut self,
        _key: <T as Table>::Key,
        _subkey: <T as DupSort>::SubKey,
    ) -> ValueOnlyResult<T> {
        self.iter.seek(zero_extend_composite_key::<T>(T::format_composite_key(
            _key.clone(),
            _subkey.clone(),
        )));

        match self.iter.item() {
            None => {
                self.state = CursorIt::End;
                Ok(None)
            }
            Some(el) => {
                self.state = CursorIt::Iterating;
                if T::unformat_key(el.0.to_vec()) == _key {
                    // TODO: why does this not include the subkey?
                    decode_value::<T>(el.1)
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn walk_dup(
        &mut self,
        _key: Option<<T>::Key>,
        _subkey: Option<<T as DupSort>::SubKey>,
    ) -> Result<DupWalker<'_, T, Self>, DatabaseError> {
        let start_el: PairResult<T> = match (_key, _subkey) {
            (None, None) => self.first(),
            (None, Some(subkey)) => {
                panic!("not implemented");
            }
            (Some(key), None) => match self.seek_exact(key)? {
                None => {
                    self.state = CursorIt::End;
                    if self.iter.valid() {
                        Err(DatabaseError::Read(DatabaseErrorInfo {
                            message: "MissingStart".into(),
                            code: 1,
                        })
                        .into())
                    } else {
                        Ok(None)
                    }
                }
                Some(el) => Ok(Some(el)),
            },
            (Some(key), Some(subkey)) => {
                match self.seek_by_key_subkey(key.clone(), subkey.clone())? {
                    None => {
                        self.state = CursorIt::End;
                        if self.iter.valid() {
                            self.iter.seek_for_prev(zero_extend_composite_key::<T>(
                                T::format_composite_key(key.clone(), subkey.clone()),
                            ));
                            match self.iter.item() {
                                None => {
                                    self.iter.seek_to_first();
                                    Err(DatabaseError::Read(DatabaseErrorInfo {
                                        message: "MissingStart".into(),
                                        code: 1,
                                    })
                                    .into())
                                }
                                Some(el) => {
                                    if T::unformat_key(el.0.to_vec()) != key {
                                        Err(DatabaseError::Read(DatabaseErrorInfo {
                                            message: "MissingStart".into(),
                                            code: 1,
                                        })
                                        .into())
                                    } else {
                                        Ok(None)
                                    }
                                }
                            }
                        } else {
                            Ok(None)
                        }
                    }
                    Some(el) => self.current(),
                }
            }
        };
        Ok(DupWalker { cursor: self, start: start_el.transpose() })
    }
}

impl<T: Table> DbCursorRW<T> for Cursor<'_, '_, T> {
    fn upsert(
        &mut self,
        _key: <T as Table>::Key,
        _value: <T as Table>::Value,
    ) -> Result<(), DatabaseError> {
        // in rocksdb its always an upsert
        let composite_key = T::format_key(_key.clone(), &_value);
        self.iter.seek_for_prev(max_extend_composite_key::<T>(composite_key.clone()));

        self.state = CursorIt::Iterating;

        match (T::TABLE.is_dupsort(), self.iter.item()) {
            (_, None) => {
                let zero_ext_key = zero_extend_composite_key::<T>(composite_key.clone());
                self.tx.put_raw::<T>(zero_ext_key.clone(), _value.compress().into())?;
                self.iter.seek(&zero_ext_key);
            }
            (false, Some(el)) => {
                self.tx.put_raw::<T>(composite_key.clone(), _value.compress().into())?;
                self.iter.seek(&composite_key);
            }
            (true, Some(el)) => {
                if unformat_extended_composite_key::<T>(el.0.to_vec()) != composite_key {
                    let zero_ext_key = zero_extend_composite_key::<T>(composite_key.clone());
                    self.tx.put_raw::<T>(zero_ext_key.clone(), _value.compress().into())?;
                    self.iter.seek(&zero_ext_key);
                } else {
                    // TODO: this is supremely inefficient. O(n) insertions.
                    // We can do O(1) amortized by keeping the indices sparse - we are
                    // inserting into a sorted vector

                    let value_to_insert = _value.compress().into();

                    while let Some(el) = self.iter.item().filter(|el| {
                        unformat_extended_composite_key::<T>(el.0.to_vec()) == composite_key
                    }) {
                        if el.1 == value_to_insert {
                            // Ignore duplicate values
                            return Ok(());
                        }
                        self.iter.prev();
                    }

                    // Reposition the cursor
                    self.iter.seek_for_prev(max_extend_composite_key::<T>(composite_key.clone()));

                    while let Some(el) = self.iter.item().filter(|el| {
                        unformat_extended_composite_key::<T>(el.0.to_vec()) == composite_key
                    }) {
                        let c_el_v = el.1.into();
                        if c_el_v < value_to_insert {
                            let inserted_key = up_extend_composite_key::<T>(el.0.to_vec());
                            self.tx.put_raw::<T>(inserted_key.clone(), value_to_insert)?;
                            self.iter.seek(inserted_key);
                            return Ok(());
                        } else {
                            self.tx.put_raw::<T>(
                                up_extend_composite_key::<T>(el.0.to_vec()),
                                c_el_v,
                            )?;
                            self.iter.prev();
                        }
                    }

                    // Lowest value - put at the front
                    let inserted_key = zero_extend_composite_key::<T>(composite_key.clone());
                    self.tx.put_raw::<T>(inserted_key.clone(), value_to_insert)?;
                    self.iter.seek(inserted_key);
                }
            }
        }

        Ok(())
    }

    fn insert(
        &mut self,
        _key: <T as Table>::Key,
        _value: <T as Table>::Value,
    ) -> Result<(), DatabaseError> {
        if self.tx.get::<T>(_key.clone())?.is_some() {
            self.iter.seek(T::format_key(_key.clone(), &_value));
            match self.iter.item() {
                None => {
                    self.iter.seek_to_last();
                    if self.iter.item().is_some() {
                        self.state = CursorIt::Iterating;
                    } else {
                        self.state = CursorIt::End;
                    }
                }
                Some(_) => {
                    self.state = CursorIt::Iterating;
                }
            }

            return Err(DatabaseError::Write(
                DatabaseWriteError {
                    info: DatabaseErrorInfo { message: "AlreadyExists".into(), code: 1 },
                    operation: DatabaseWriteOperation::CursorInsert,
                    table_name: T::NAME,
                    key: _key.encode().into(),
                }
                .into(),
            ));
        }
        self.upsert(_key, _value)
    }

    fn append(
        &mut self,
        _key: <T as Table>::Key,
        _value: <T as Table>::Value,
    ) -> Result<(), DatabaseError> {
        let last_el = self.last();
        match last_el {
            Err(e) => Err(DatabaseWriteError {
                info: DatabaseErrorInfo { message: e.to_string(), code: 1 },
                operation: DatabaseWriteOperation::CursorAppend,
                table_name: T::NAME,
                key: _key.encode().into(),
            }
            .into()),
            Ok(None) => self.upsert(_key, _value),
            Ok(Some(_item)) => {
                if _item.0 > _key {
                    return Err(DatabaseWriteError {
                        info: DatabaseErrorInfo { message: "KeyMismatch".into(), code: 1 },
                        operation: DatabaseWriteOperation::CursorAppend,
                        table_name: T::NAME,
                        key: _key.encode().into(),
                    }
                    .into());
                }
                self.upsert(_key, _value)
            }
        }
    }

    fn delete_current(&mut self) -> Result<(), DatabaseError> {
        // TODO: should delete_current delete all duplicates as well?
        match self.state {
            CursorIt::Start => Err(DatabaseError::Read(DatabaseErrorInfo {
                message: "Deleting from uninitialized cursor".into(),
                code: 1,
            })
            .into()),
            CursorIt::End => Ok(()),
            CursorIt::Iterating => match self.iter.key().map(|k| k.to_vec()) {
                None => Ok(()),
                Some(key) => {
                    let locked_opt_tx = self.tx.inner.lock().unwrap();
                    let tx = locked_opt_tx.as_ref().unwrap();
                    let cf_handle = self.tx.db.cf_handle(&String::from(T::NAME)).unwrap();

                    let _ = tx.delete_cf(cf_handle, &key);
                    self.iter.seek(&key);
                    match self.iter.item() {
                        None => {
                            self.state = CursorIt::End;
                        }
                        Some(el) => {
                            if self.dup_mode {
                                self.state = CursorIt::Iterating;
                                if T::unformat_key(el.0.to_vec()) != T::unformat_key(key) {
                                    self.state = CursorIt::End;
                                    // self.iter.prev();
                                }
                            } else {
                                self.state = CursorIt::Iterating;
                            }
                        }
                    }
                    Ok(())
                }
            },
        }
    }
}

impl<T: DupSort> DbDupCursorRW<T> for Cursor<'_, '_, T> {
    fn delete_current_duplicates(&mut self) -> Result<(), DatabaseError> {
        match self.state {
            CursorIt::Start => Err(DatabaseError::Read(DatabaseErrorInfo {
                message: "Deleting from uninitialized cursor".into(),
                code: 1,
            })
            .into()),
            CursorIt::End => Ok(()),
            CursorIt::Iterating => {
                let start_ext_key = self.iter.key().unwrap().to_vec();
                let current_primary = T::unformat_key(start_ext_key.clone());
                self.iter.seek(current_primary.clone().encode().as_ref());

                let cf_handle = self.tx.db.cf_handle(&String::from(T::NAME)).unwrap();

                let mut to_delete: Vec<Vec<u8>> = Vec::new();
                while let Some(key) = self
                    .iter
                    .key()
                    .map(|k| k.to_vec())
                    .filter(|k| T::unformat_key(k.to_owned()) == current_primary)
                {
                    to_delete.push(key.to_owned());
                    self.iter.next();
                }

                let locked_opt_tx = self.tx.inner.lock().unwrap();
                let tx = locked_opt_tx.as_ref().unwrap();
                for key in to_delete {
                    let _ = tx.delete_cf(cf_handle, key);
                }

                let _ = self.iter.seek(current_primary.encode().as_ref());
                if self.iter.valid() {
                    self.state = CursorIt::Iterating;
                } else {
                    self.state = CursorIt::End;
                }

                return Ok(());
            }
        }
    }

    fn append_dup(&mut self, _key: <T>::Key, _value: <T>::Value) -> Result<(), DatabaseError> {
        let composite_key_to_insert = T::format_key(_key.clone(), &_value);
        let ext_key: Vec<u8> = zero_extend_composite_key::<T>(composite_key_to_insert.clone());

        self.iter.seek(&ext_key);
        match self.iter.item() {
            None => self.upsert(_key, _value),
            Some(el) => {
                if T::unformat_key(el.0.to_vec()) != _key {
                    return self.upsert(_key, _value);
                }

                if unformat_extended_composite_key::<T>(el.0.to_vec()) < composite_key_to_insert {
                    return self.upsert(_key, _value);
                }

                if unformat_extended_composite_key::<T>(el.0.to_vec()) > composite_key_to_insert {
                    return Err(DatabaseWriteError {
                        info: DatabaseErrorInfo { message: "KeyMismatch".into(), code: 1 },
                        operation: DatabaseWriteOperation::CursorAppendDup,
                        table_name: T::NAME,
                        key: _key.encode().into(),
                    }
                    .into());
                }

                // Check highest dup value
                let ext_key: Vec<u8> =
                    max_extend_composite_key::<T>(composite_key_to_insert.clone());
                self.iter.seek_for_prev(&ext_key);
                match self.iter.item() {
                    None => self.upsert(_key, _value), // weird
                    Some(el) => {
                        let value_to_insert: Vec<u8> = _value.compress().into();
                        if el.1 > value_to_insert.as_slice() {
                            Err(DatabaseWriteError {
                                info: DatabaseErrorInfo { message: "KeyMismatch".into(), code: 1 },
                                operation: DatabaseWriteOperation::CursorAppendDup,
                                table_name: T::NAME,
                                key: _key.encode().into(),
                            }
                            .into())
                        } else if el.1 == value_to_insert.as_slice() {
                            Ok(())
                        } else {
                            let inserted_ext_key = up_extend_composite_key::<T>(el.0.to_vec());
                            self.tx.put_raw::<T>(inserted_ext_key.clone(), value_to_insert)?;
                            self.iter.seek(&inserted_ext_key);
                            self.state = CursorIt::Iterating;
                            Ok(())
                        }
                    }
                }
            }
        }
    }
}

pub fn decode_item<T>(res: Option<(&[u8], &[u8])>) -> PairResult<T>
where
    T: Table,
    T::Key: Decode,
    T::Value: Decompress,
{
    match res {
        None => Ok(None),
        Some(el) => {
            let key = T::unformat_key(el.0.to_vec());
            let value = decode_one::<T>(Cow::Owned(el.1.to_vec())).map_err(|e| {
                DatabaseError::Read(DatabaseErrorInfo { message: e.to_string(), code: 1 })
            })?;
            Ok(Some((key, value)))
        }
    }
}

pub fn decode_value<T>(v: &[u8]) -> Result<Option<T::Value>, DatabaseError>
where
    T: Table,
    T::Key: Decode,
    T::Value: Decompress,
{
    Some(decode_one::<T>(Cow::Owned(v.to_vec()))).transpose()
}
