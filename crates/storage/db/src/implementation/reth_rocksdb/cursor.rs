use crate::{
    common::{IterPairResult, PairResult, ValueOnlyResult},
    cursor::{
        DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW, DupWalker, RangeWalker,
        ReverseWalker, Walker,
    },
    database::Database,
    reth_rocksdb,
    table::{Compress, Decode, Decompress, DupSort, Encode, KeyFormat, Table, TableImporter},
    tables::utils::{decode_one, decoder},
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};

use core::ops::Bound;
use core::ops::Deref;
use reth_interfaces::db::DatabaseErrorInfo;
use reth_interfaces::db::{DatabaseWriteError, DatabaseWriteOperation};
use std::borrow::Borrow;
use std::borrow::Cow;
use std::fmt;
use std::{collections::BTreeMap, ops::RangeBounds};

use rocksdb;

/// Cursor that iterates over table
pub struct Cursor<'itx, 'it, T: Table> {
    pub iter:
        rocksdb::DBIteratorWithThreadMode<'it, rocksdb::Transaction<'it, rocksdb::TransactionDB>>,
    pub tx: &'itx reth_rocksdb::tx::Tx<'it, rocksdb::TransactionDB>,
    // TODO: use peekable once it's not unstable
    pub current: Option<Box<[u8]>>,
    table_type: std::marker::PhantomData<T>,
}

impl<'itx, 'it: 'itx, T: Table> Cursor<'itx, 'it, T> {
    pub fn new(
        iter: rocksdb::DBIteratorWithThreadMode<
            'it,
            rocksdb::Transaction<'_, rocksdb::TransactionDB>,
        >,
        tx: &'itx reth_rocksdb::tx::Tx<'it, rocksdb::TransactionDB>,
    ) -> Cursor<'itx, 'it, T> {
        Self { iter, tx, current: None, table_type: std::marker::PhantomData }
    }

    // Note: self.current should hold the extended dup key!
    fn update_current_and_decode(
        &mut self,
        res: Option<Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>>,
    ) -> PairResult<T> {
        match res {
            None => Ok(None),
            Some(r) => match r {
                Err(e) => {
                    Err(DatabaseError::Read(DatabaseErrorInfo { message: e.to_string(), code: 1 }))
                }
                Ok(item) => {
                    self.current = Some(item.0.clone());
                    match T::TABLE.is_dupsort() {
                        true => {
                            let key =
                                <T as KeyFormat<T::Key, T::Value>>::unformat_key(item.0.into_vec());
                            let value =
                                decode_one::<T>(Cow::Owned(item.1.into_vec())).map_err(|e| {
                                    DatabaseError::Read(DatabaseErrorInfo {
                                        message: e.to_string(),
                                        code: 1,
                                    })
                                })?;
                            Ok(Some((key, value)))
                        }
                        false => Some(decoder::<T>((
                            Cow::Owned(item.0.into_vec()),
                            Cow::Owned(item.1.into_vec()),
                        )))
                        .transpose(),
                    }
                }
            },
        }
    }
}

impl<T: Table> fmt::Debug for Cursor<'_, '_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cursor").finish()
    }
}

impl<T: Table> DbCursorRO<T> for Cursor<'_, '_, T> {
    fn first(&mut self) -> PairResult<T> {
        self.iter.set_mode(rocksdb::IteratorMode::Start);

        let it_el = self.iter.next();
        self.update_current_and_decode(it_el)
    }

    fn seek_exact(&mut self, _key: T::Key) -> PairResult<T> {
        let encoded_key = _key.clone().encode();
        self.iter.set_mode(rocksdb::IteratorMode::From(
            encoded_key.as_ref(),
            rocksdb::Direction::Forward,
        ));

        let it_el = self.iter.find(|res| match res {
            Err(_e) => false,
            Ok((k, _v)) => {
                if T::TABLE.is_dupsort() {
                    return encoded_key.as_ref()
                        <= k.as_ref().split_at(encoded_key.as_ref().len()).0;
                } else {
                    return encoded_key.as_ref() <= k.deref();
                }
            }
        });
        match it_el {
            None => {
                // TODO: should be the extended key
                self.current = Some(encoded_key.as_ref().into());
                Ok(None)
            }
            Some(Err(e)) => {
                Err(DatabaseError::Read(DatabaseErrorInfo { message: e.to_string(), code: 1 }))
            }
            Some(Ok((k, v))) => {
                self.current = Some(k.clone());
                if T::TABLE.is_dupsort() {
                    if encoded_key.as_ref() == k.as_ref().split_at(encoded_key.as_ref().len()).0 {
                        return Ok(Some((_key, decode_one::<T>(Cow::Owned(v.into_vec()))?)));
                    }
                } else {
                    if encoded_key.as_ref() == k.deref() {
                        return Ok(Some((_key, decode_one::<T>(Cow::Owned(v.into_vec()))?)));
                    }
                }
                Ok(None)
            }
        }
    }

    fn seek(&mut self, _key: T::Key) -> PairResult<T> {
        let encoded_key = _key.clone().encode();
        self.iter.set_mode(rocksdb::IteratorMode::From(
            encoded_key.as_ref(),
            rocksdb::Direction::Forward,
        ));
        let it_el = self.iter.find(|res| match res {
            Err(_e) => false,
            Ok((k, _v)) => {
                if T::TABLE.is_dupsort() {
                    return encoded_key.as_ref()
                        <= k.as_ref().split_at(encoded_key.as_ref().len()).0;
                } else {
                    return encoded_key.as_ref() <= k.deref();
                }
            }
        });
        match it_el {
            None => {
                // TODO: should be extended (mark after the highest key-subkey)
                // Either needs to be an enum or we can do encoded_key+subkey of last
                self.current = Some(encoded_key.as_ref().into());
                Ok(None)
            }
            Some(Err(e)) => {
                Err(DatabaseError::Read(DatabaseErrorInfo { message: e.to_string(), code: 1 }))
            }
            Some(Ok(it_r)) => {
                self.current = Some(it_r.0.clone());
                decode::<T>(Some(Ok(it_r)))
            }
        }
    }

    fn next(&mut self) -> PairResult<T> {
        let it_el = self.iter.next();
        self.update_current_and_decode(it_el)
    }

    fn prev(&mut self) -> PairResult<T> {
        match self.current.clone() {
            None => self.last(),
            Some(current_key) => {
                self.iter.set_mode(rocksdb::IteratorMode::From(
                    &current_key,
                    rocksdb::Direction::Reverse,
                ));
                let it_el = (|| {
                    // Skips current_key
                    let c_it_el = self.iter.next();
                    if let Some(Ok(c_it_kv)) = c_it_el {
                        if c_it_kv.0.clone() != current_key {
                            return Some(Ok(c_it_kv));
                        }
                    }
                    self.iter.next()
                })();
                let r = self.update_current_and_decode(it_el);

                match self.current.clone() {
                    None => {
                        self.iter.set_mode(rocksdb::IteratorMode::Start);
                    }
                    Some(c) => {
                        self.iter
                            .set_mode(rocksdb::IteratorMode::From(&c, rocksdb::Direction::Forward));
                        // Should we call next?
                    }
                }
                r
            }
        }
    }

    fn last(&mut self) -> PairResult<T> {
        self.iter.set_mode(rocksdb::IteratorMode::End);
        let last_el = self.iter.next();
        let r = self.update_current_and_decode(last_el);
        match self.current.clone() {
            None => {
                self.iter.set_mode(rocksdb::IteratorMode::Start);
            }
            Some(c) => {
                self.iter.set_mode(rocksdb::IteratorMode::From(&c, rocksdb::Direction::Forward));
            }
        }
        r
    }

    fn current(&mut self) -> PairResult<T> {
        match &self.current {
            None => Ok(None),
            Some(current_key) => {
                self.iter.set_mode(rocksdb::IteratorMode::From(
                    &current_key,
                    rocksdb::Direction::Forward,
                ));
                // TODO: shouldn't this be just next()?
                let it_el = self.iter.find(|res| match res {
                    Err(_e) => false,
                    Ok((k, _v)) => {
                        return current_key.as_ref() == k.deref();
                    }
                });
                decode::<T>(it_el)
            }
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

        match start_key {
            None => self.iter.set_mode(rocksdb::IteratorMode::Start),
            Some(key) => {
                self.iter.set_mode(rocksdb::IteratorMode::From(
                    key.encode().as_ref(),
                    rocksdb::Direction::Forward,
                ));
            }
        };

        let start_item = self.next().transpose();
        Ok(RangeWalker::new(self, start_item, range.end_bound().cloned()))
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError> {
        let start: IterPairResult<T> = match start_key {
            Some(key) => self.seek(key).transpose(),
            None => self.last().transpose(),
        };
        Ok(ReverseWalker::new(self, start))
    }
}

impl<T: DupSort> DbDupCursorRO<T> for Cursor<'_, '_, T> {
    fn next_dup(&mut self) -> PairResult<T> {
        if self.current.is_none() {
            return self.next();
        }

        let current_key = self.current.clone().unwrap();
        match self.next() {
            Err(e) => Err(e),
            Ok(None) => Ok(None),
            Ok(Some(nv)) => {
                let encoded_key = nv.0.clone().encode();
                match encoded_key.as_ref()
                    == current_key.as_ref().split_at(encoded_key.as_ref().len()).0
                {
                    true => Ok(Some(nv)),
                    false => Ok(None), // Next primary key, return None indicating we are done
                                       // iterating
                }
            }
        }
    }

    fn next_no_dup(&mut self) -> PairResult<T> {
        if self.current.is_none() {
            return self.next();
        }

        let current_key = self.current.clone().unwrap();
        loop {
            //TODO: should be a seek
            let (stop, ni) = match self.next() {
                Err(e) => (true, Err(e)),
                Ok(None) => (true, Ok(None)),
                Ok(Some(nv)) => {
                    let encoded_key = nv.0.clone().encode();
                    match encoded_key.as_ref()
                        == current_key.as_ref().split_at(encoded_key.as_ref().len()).0
                    {
                        true => (false, Ok(None)), // Still the same primary key, continue
                        false => (true, Ok(Some(nv))),
                    }
                }
            };
            if stop {
                return ni;
            }
        }
    }

    fn next_dup_val(&mut self) -> ValueOnlyResult<T> {
        let current_key = self.current.clone();
        match self.next() {
            Err(e) => Err(e),
            Ok(None) => Ok(None),
            Ok(Some((k, v))) => match current_key {
                None => Ok(Some(v)),
                Some(mk) => {
                    let encoded_key = k.encode();
                    match encoded_key.as_ref() == mk.as_ref().split_at(encoded_key.as_ref().len()).0
                    {
                        true => Ok(Some(v)),
                        false => Ok(None),
                    }
                }
            },
        }
    }

    fn seek_by_key_subkey(
        &mut self,
        _key: <T as Table>::Key,
        _subkey: <T as DupSort>::SubKey,
    ) -> ValueOnlyResult<T> {
        let encoded_key = _key.encode();
        let mut ext_key: Vec<u8> = encoded_key.as_ref().into();
        let subkey_vec: Vec<u8> = _subkey.encode().as_ref().into();
        ext_key.extend_from_slice(subkey_vec.as_slice());
        self.iter
            .set_mode(rocksdb::IteratorMode::From(ext_key.as_slice(), rocksdb::Direction::Forward));

        let it_el = self.iter.find(|res| match res {
            Err(_e) => false,
            Ok((k, _v)) => {
                return ext_key.as_slice() <= k.as_ref();
            }
        });
        match it_el {
            None => {
                self.current = Some(ext_key.into());
                Ok(None)
            }
            Some(Err(e)) => {
                Err(DatabaseError::Read(DatabaseErrorInfo { message: e.to_string(), code: 1 }))
            }
            Some(Ok(it_r)) => {
                self.current = Some(it_r.0.clone());
                if encoded_key.as_ref() == it_r.0.as_ref().split_at(encoded_key.as_ref().len()).0 {
                    return decode_value::<T>(Some(Ok(it_r)));
                }
                Ok(None)
            }
        }
    }

    fn walk_dup(
        &mut self,
        _key: Option<<T>::Key>,
        _subkey: Option<<T as DupSort>::SubKey>,
    ) -> Result<DupWalker<'_, T, Self>, DatabaseError> {
        if _key.is_none() {
            self.iter.set_mode(rocksdb::IteratorMode::Start);
        } else {
            // TODO: add format key from key, subkey for dup table type!
            let encoded_key = _key.unwrap().encode();
            if _subkey.is_none() {
                self.iter.set_mode(rocksdb::IteratorMode::From(
                    encoded_key.as_ref(),
                    rocksdb::Direction::Forward,
                ));
            }
            let mut ext_key: Vec<u8> = encoded_key.as_ref().into();
            let subkey_vec: Vec<u8> = _subkey.unwrap().encode().as_ref().into();
            ext_key.extend_from_slice(subkey_vec.as_slice());
            self.iter.set_mode(rocksdb::IteratorMode::From(
                ext_key.as_slice(),
                rocksdb::Direction::Forward,
            ));
        }
        Ok(DupWalker { cursor: self, start: None })
    }
}

impl<T: Table> DbCursorRW<T> for Cursor<'_, '_, T> {
    fn upsert(
        &mut self,
        _key: <T as Table>::Key,
        _value: <T as Table>::Value,
    ) -> Result<(), DatabaseError> {
        // in rocksdb its always an upsert
        let ext_key =
            Some(<T as KeyFormat<T::Key, T::Value>>::format_key(_key.clone(), &_value).into());
        self.tx.put::<T>(_key, _value)?;
        self.current = ext_key;
        Ok(())
    }

    fn insert(
        &mut self,
        _key: <T as Table>::Key,
        _value: <T as Table>::Value,
    ) -> Result<(), DatabaseError> {
        if self.tx.get::<T>(_key.clone())?.is_some() {
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
        match self.current.clone() {
            None => {
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
                    Ok(Some(item)) => self.append(_key, _value),
                }
            }
            Some(ck) => {
                let ext_key = _key.clone().encode();
                if ck.split_at(ext_key.as_ref().len()).0 > ext_key.as_ref() {
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
        match self.current.clone() {
            None => {
                panic!("should never happen")
            }
            Some(ck) => {
                let locked_opt_tx = self.tx.inner.lock().unwrap();
                let tx = locked_opt_tx.as_ref().unwrap();
                let cf_handle = self.tx.db.cf_handle(&String::from(T::NAME)).unwrap();

                let _ = tx.delete_cf(cf_handle, &ck);
                Ok(())
            }
        }
    }
}

impl<T: DupSort> DbDupCursorRW<T> for Cursor<'_, '_, T> {
    fn delete_current_duplicates(&mut self) -> Result<(), DatabaseError> {
        Ok(()) // NOOP in rocksdb
    }

    fn append_dup(&mut self, _key: <T>::Key, _value: <T>::Value) -> Result<(), DatabaseError> {
        match self.current.clone() {
            None => {
                let last_el = self.last(); // TODO: last for this primary key?
                match last_el {
                    Err(e) => Err(DatabaseWriteError {
                        info: DatabaseErrorInfo { message: e.to_string(), code: 1 },
                        operation: DatabaseWriteOperation::CursorAppendDup,
                        table_name: T::NAME,
                        key: _key.encode().into(),
                    }
                    .into()),
                    Ok(None) => self.upsert(_key, _value),
                    Ok(Some(item)) => self.append_dup(_key, _value),
                }
            }
            Some(ck) => {
                let ext_key = <T as KeyFormat<T::Key, T::Value>>::format_key(_key.clone(), &_value);
                if ck.split_at(ext_key.len()).0 > ext_key.as_slice() {
                    return Err(DatabaseWriteError {
                        info: DatabaseErrorInfo { message: "KeyMismatch".into(), code: 1 },
                        operation: DatabaseWriteOperation::CursorAppendDup,
                        table_name: T::NAME,
                        key: _key.encode().into(),
                    }
                    .into());
                }
                self.upsert(_key, _value)
            }
        }
    }
}

/*
pub fn decode<T>(res: Option<(Box<[u8]>, Box<[u8]>)>) -> PairResult<T>
where
    T: Table,
    T::Key: Decode,
    T::Value: Decompress,
{
    match res {
        None => Ok(None),
        Some(item) => {
            Some(decoder::<T>((Cow::Owned(item.0.into_vec()), Cow::Owned(item.1.into_vec()))))
                .transpose()
        }
    }
}
*/

pub fn decode<T>(res: Option<Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>>) -> PairResult<T>
where
    T: Table,
    T::Key: Decode,
    T::Value: Decompress,
{
    match res {
        None => Ok(None),
        Some(r) => match r {
            Err(e) => {
                Err(DatabaseError::Read(DatabaseErrorInfo { message: e.to_string(), code: 1 }))
            }
            Ok(item) => match T::TABLE.is_dupsort() {
                true => {
                    let key = <T as KeyFormat<T::Key, T::Value>>::unformat_key(item.0.into_vec());
                    let value = decode_one::<T>(Cow::Owned(item.1.into_vec())).map_err(|e| {
                        DatabaseError::Read(DatabaseErrorInfo { message: e.to_string(), code: 1 })
                    })?;
                    Ok(Some((key, value)))
                }
                false => Some(decoder::<T>((
                    Cow::Owned(item.0.into_vec()),
                    Cow::Owned(item.1.into_vec()),
                )))
                .transpose(),
            },
        },
    }
}

pub fn decode_value<T>(
    res: Option<Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>>,
) -> Result<Option<T::Value>, DatabaseError>
where
    T: Table,
    T::Key: Decode,
    T::Value: Decompress,
{
    match res {
        None => Ok(None),
        Some(r) => match r {
            Err(e) => {
                Err(DatabaseError::Read(DatabaseErrorInfo { message: e.to_string(), code: 1 }))
            }
            Ok(item) => Some(decode_one::<T>(Cow::Owned(item.1.into_vec()))).transpose(),
        },
    }
}
