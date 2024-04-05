use crate::{
    common::{IterPairResult, PairResult, ValueOnlyResult},
    cursor::{
        self, DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW, DupWalker, RangeWalker,
        ReverseWalker, Walker,
    },
    reth_rocksdb,
    table::{Decode, Decompress, DupSort, Encode, KeyFormat, Table},
    tables::utils::{decode_one, decoder},
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};

use core::ops::Bound;
use core::ops::Deref;
use reth_interfaces::db::DatabaseErrorInfo;
use reth_interfaces::db::{DatabaseWriteError, DatabaseWriteOperation};
use reth_primitives::ForkId;

use std::fmt;
use std::ops::RangeBounds;
use std::{borrow::Cow, iter::Rev};

use rocksdb;

use rocksdb::Direction::{self, Forward, Reverse};

#[derive(Clone)]
enum CursorIt {
    Start(Direction),
    End(Direction),
    Key(Box<[u8]>, Direction),
}

fn cursor_direction(c: &CursorIt) -> Direction {
    match c {
        CursorIt::Start(d) => *d,
        CursorIt::End(d) => *d,
        CursorIt::Key(_, d) => *d,
    }
}

fn change_direction_if_needed(c: CursorIt, nd: Direction) -> CursorIt {
    match nd {
        Forward => match c {
            CursorIt::Start(Reverse) => CursorIt::Start(nd),
            CursorIt::End(Reverse) => CursorIt::End(nd),
            CursorIt::Key(k, Reverse) => CursorIt::Key(k, nd),
            _ => c,
        },
        Reverse => match c {
            CursorIt::Start(Forward) => CursorIt::Start(nd),
            CursorIt::End(Forward) => CursorIt::End(nd),
            CursorIt::Key(k, Forward) => CursorIt::Key(k, nd),
            _ => c,
        },
    }
}

/// Cursor that iterates over table
pub struct Cursor<'itx, 'it, T: Table> {
    pub iter:
        rocksdb::DBIteratorWithThreadMode<'it, rocksdb::Transaction<'it, rocksdb::TransactionDB>>,
    pub tx: &'itx reth_rocksdb::tx::Tx<'it, rocksdb::TransactionDB>,
    // TODO: use peekable once it's not unstable
    pub current: CursorIt,
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
        Self { iter, tx, current: CursorIt::Start(Forward), table_type: std::marker::PhantomData }
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
        self.current = CursorIt::Start(Forward);

        match self.iter.next() {
            None => Ok(None),
            Some(Err(e)) => {
                Err(DatabaseError::Read(DatabaseErrorInfo { message: e.to_string(), code: 1 }))
            }
            Some(Ok(el)) => {
                self.current = CursorIt::Key(el.0.clone(), Forward);
                decode::<T>(Some(Ok(el)))
            }
        }
    }

    fn seek_exact(&mut self, _key: T::Key) -> PairResult<T> {
        let encoded_key = _key.clone().encode();
        self.iter.set_mode(rocksdb::IteratorMode::From(
            encoded_key.as_ref(),
            rocksdb::Direction::Forward,
        ));

        match self.iter.next() {
            None => {
                self.current = CursorIt::End(Forward);
                Ok(None)
            }
            Some(Err(e)) => {
                Err(DatabaseError::Read(DatabaseErrorInfo { message: e.to_string(), code: 1 }))
            }
            Some(Ok(n_el)) => {
                self.current = CursorIt::Key(n_el.0.clone(), Forward);
                match encoded_key.as_ref() == n_el.0.as_ref().split_at(encoded_key.as_ref().len()).0
                {
                    true => decode::<T>(Some(Ok(n_el))),
                    false => Ok(None),
                }
            }
        }
    }

    fn seek(&mut self, _key: T::Key) -> PairResult<T> {
        let encoded_key = _key.clone().encode();
        self.iter.set_mode(rocksdb::IteratorMode::From(encoded_key.as_ref(), Forward));
        match self.iter.next() {
            None => {
                self.current = CursorIt::End(Forward);
                Ok(None)
            }
            Some(Err(e)) => {
                Err(DatabaseError::Read(DatabaseErrorInfo { message: e.to_string(), code: 1 }))
            }
            Some(Ok(it_r)) => {
                self.current = CursorIt::Key(it_r.0.clone(), Forward);
                decode::<T>(Some(Ok(it_r)))
            }
        }
    }

    fn next(&mut self) -> PairResult<T> {
        match self.current.clone() {
            CursorIt::Start(_) => self.first(),
            CursorIt::End(_) => Ok(None),
            CursorIt::Key(_, Forward) => match self.iter.next() {
                None => {
                    self.current = CursorIt::End(Forward);
                    Ok(None)
                }
                Some(Err(e)) => {
                    Err(DatabaseError::Read(DatabaseErrorInfo { message: e.to_string(), code: 1 }))
                }
                Some(Ok(n_el)) => {
                    self.current = CursorIt::Key(n_el.0.clone(), Forward);
                    decode::<T>(Some(Ok(n_el)))
                }
            },
            CursorIt::Key(current_key, Reverse) => {
                self.iter.set_mode(rocksdb::IteratorMode::From(&current_key, Forward));
                self.current = CursorIt::Key(current_key, Forward);
                self.next()
            }
        }
    }

    fn prev(&mut self) -> PairResult<T> {
        match self.current.clone() {
            CursorIt::Start(_) => self.last(), // TODO: should this be Ok(None)?
            CursorIt::End(Forward) => {
                self.current = CursorIt::End(Reverse);
                self.prev()
            }
            CursorIt::End(Reverse) => {
                self.iter.set_mode(rocksdb::IteratorMode::End);
                match self.iter.next() {
                    None => {
                        self.iter.set_mode(rocksdb::IteratorMode::Start);
                        self.current = CursorIt::Start(Forward);
                        Ok(None)
                    }
                    Some(Err(e)) => Err(DatabaseError::Read(DatabaseErrorInfo {
                        message: e.to_string(),
                        code: 1,
                    })),
                    Some(Ok(r_el)) => {
                        self.current = CursorIt::Key(r_el.0.clone(), Reverse);
                        decode::<T>(Some(Ok(r_el)))
                    }
                }
            }
            CursorIt::Key(current_key, Reverse) => {
                let n_it = (|| {
                    // Skips current_key
                    let c_it = self.iter.next();
                    if let Some(Ok(c_it_el)) = c_it {
                        if c_it_el.0 != current_key {
                            return Some(Ok(c_it_el));
                        }
                    }
                    self.iter.next()
                })();

                match n_it {
                    None => {
                        self.current = CursorIt::Start(Reverse);
                        Ok(None)
                    }
                    Some(Err(e)) => Err(DatabaseError::Read(DatabaseErrorInfo {
                        message: e.to_string(),
                        code: 1,
                    })),
                    Some(Ok(it_el)) => {
                        self.current = CursorIt::Key(it_el.0.clone(), Reverse);
                        decode::<T>(Some(Ok(it_el)))
                    }
                }
            }
            CursorIt::Key(current_key, Forward) => {
                self.iter.set_mode(rocksdb::IteratorMode::From(
                    &current_key,
                    rocksdb::Direction::Reverse,
                ));
                self.current = CursorIt::Key(current_key, Reverse);
                self.prev()
            }
        }
    }

    fn last(&mut self) -> PairResult<T> {
        self.iter.set_mode(rocksdb::IteratorMode::End);
        match self.iter.next() {
            None => {
                self.iter.set_mode(rocksdb::IteratorMode::Start);
                self.current = CursorIt::End(Forward);
                Ok(None)
            }
            Some(Err(e)) => {
                Err(DatabaseError::Read(DatabaseErrorInfo { message: e.to_string(), code: 1 }))
            }
            Some(Ok(last_el)) => {
                self.current = CursorIt::Key(last_el.0.clone(), cursor_direction(&self.current));
                self.iter
                    .set_mode(rocksdb::IteratorMode::From(&last_el.0, rocksdb::Direction::Forward));
                decode::<T>(Some(Ok(last_el)))
            }
        }
    }

    fn current(&mut self) -> PairResult<T> {
        match &self.current {
            CursorIt::Start(_) => {
                Ok(None) // Should this not be first?
            }
            CursorIt::End(_) => Ok(None),
            CursorIt::Key(current_key, _) => {
                self.iter.set_mode(rocksdb::IteratorMode::From(
                    &current_key,
                    cursor_direction(&self.current),
                ));
                let it_el = self.iter.next(); // Note that this might not be the same as
                                              // self.current as it might have been deleted

                // TODO: Note that the iter points at the next element now
                // Not sure if we have to reposition it
                self.iter.set_mode(rocksdb::IteratorMode::From(
                    &current_key,
                    cursor_direction(&self.current),
                ));

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

        let start_item = match start_key {
            None => {
                self.iter.set_mode(rocksdb::IteratorMode::Start);
                self.current = CursorIt::Start(Forward);
                self.next().transpose()
            }
            Some(key) => self.seek(key).transpose(),
        };

        Ok(RangeWalker::new(self, start_item, range.end_bound().cloned()))
    }

    fn walk_back(
        &mut self,
        start_key: Option<T::Key>,
    ) -> Result<ReverseWalker<'_, T, Self>, DatabaseError> {
        let start: IterPairResult<T> = match start_key {
            None => {
                self.iter.set_mode(rocksdb::IteratorMode::End);
                self.current = CursorIt::End(Reverse);
                self.prev().transpose()
            }
            Some(key) => self.seek(key).transpose(),
        };
        Ok(ReverseWalker::new(self, start))
    }
}

impl<T: DupSort> DbDupCursorRO<T> for Cursor<'_, '_, T> {
    fn next_dup(&mut self) -> PairResult<T> {
        match self.current.clone() {
            CursorIt::Start(Forward) => self.next(),
            CursorIt::Start(Reverse) => {
                self.iter.set_mode(rocksdb::IteratorMode::Start);
                self.current = CursorIt::Start(Forward);
                self.next_dup()
            }
            CursorIt::End(_) => Ok(None),
            CursorIt::Key(current_key, Forward) => {
                match self.iter.next() {
                    None => {
                        self.current = CursorIt::End(Forward);
                        Ok(None)
                    }
                    Some(Err(e)) => Err(DatabaseError::Read(DatabaseErrorInfo {
                        message: e.to_string(),
                        code: 1,
                    })),
                    Some(Ok(n_el)) => {
                        self.current = CursorIt::Key(n_el.0.clone(), Forward);

                        let current_primary = <T as KeyFormat<T::Key, T::Value>>::unformat_key(
                            current_key.into_vec(),
                        );
                        let n_el_primary = <T as KeyFormat<T::Key, T::Value>>::unformat_key(
                            n_el.0.clone().into_vec(),
                        );
                        match current_primary == n_el_primary {
                            true => decode::<T>(Some(Ok(n_el))),
                            false => Ok(None), // Next primary key, return None indicating we are done
                                               // iterating
                        }
                    }
                }
            }
            CursorIt::Key(current_key, Reverse) => {
                self.iter.set_mode(rocksdb::IteratorMode::From(current_key.as_ref(), Forward));
                self.current = CursorIt::Key(current_key.clone(), Forward);
                // TODO: should we call next to skip current?
                self.next_dup()
            }
        }
    }

    fn next_no_dup(&mut self) -> PairResult<T> {
        match self.current.clone() {
            CursorIt::Start(Forward) => self.next(),
            CursorIt::Start(Reverse) => {
                self.iter.set_mode(rocksdb::IteratorMode::Start);
                self.current = CursorIt::Start(Forward);
                self.next()
            }
            CursorIt::End(_) => Ok(None),
            CursorIt::Key(current_key, Forward) => {
                self.iter.set_mode(rocksdb::IteratorMode::From(
                    &current_key,
                    rocksdb::Direction::Forward,
                ));

                let current_primary_as_bytes = <T as KeyFormat<T::Key, T::Value>>::unformat_key(
                    current_key.clone().into_vec(),
                )
                .encode();

                let first_after_current_it = self.iter.find(|res| match res {
                    Err(_e) => false,
                    Ok((k, _v)) => {
                        current_primary_as_bytes.as_ref()
                            < k.as_ref().split_at(current_key.as_ref().len()).0
                    }
                });

                match first_after_current_it {
                    None => {
                        self.current = CursorIt::End(Forward);
                        Ok(None)
                    }
                    Some(Err(e)) => Err(DatabaseError::Read(DatabaseErrorInfo {
                        message: e.to_string(),
                        code: 1,
                    })),
                    Some(Ok(first_after_current_el)) => {
                        self.current = CursorIt::Key(first_after_current_el.0.clone(), Forward);
                        decode::<T>(Some(Ok(first_after_current_el)))
                    }
                }
            }
            CursorIt::Key(current_key, Reverse) => {
                self.iter.set_mode(rocksdb::IteratorMode::From(current_key.as_ref(), Forward));
                self.current = CursorIt::Key(current_key.clone(), Forward);
                // TODO: should we call next to skip current?
                self.next_no_dup()
            }
        }
    }

    fn next_dup_val(&mut self) -> ValueOnlyResult<T> {
        // TODO: this is not correctly implemented for non-unique composite (primary, sub), might break bundle state provider
        match self.next_dup() {
            Err(e) => Err(e),
            Ok(None) => Ok(None),
            Ok(Some(n_el)) => Ok(Some(n_el.1.into())),
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

        match self.iter.next() {
            None => {
                self.current = CursorIt::End(Forward);
                Ok(None)
            }
            Some(Err(e)) => {
                let next_current = change_direction_if_needed(self.current.clone(), Forward);
                self.current = next_current;
                Err(DatabaseError::Read(DatabaseErrorInfo { message: e.to_string(), code: 1 }))
            }
            Some(Ok(it_r)) => {
                self.current = CursorIt::Key(it_r.0.clone(), Forward);
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
        let start_el = match (_key, _subkey) {
            (None, _) => self.first(),
            (Some(key), None) => self.seek(key),
            (Some(key), Some(subkey)) => {
                let mut ext_key: Vec<u8> = key.encode().as_ref().into();
                let subkey_vec: Vec<u8> = subkey.encode().as_ref().into();
                ext_key.extend_from_slice(subkey_vec.as_slice());
                self.iter.set_mode(rocksdb::IteratorMode::From(&ext_key, Forward));
                self.current = CursorIt::Key(ext_key.into(), Forward);
                self.next()
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
        let ext_key = <T as KeyFormat<T::Key, T::Value>>::format_key(_key.clone(), &_value);
        self.tx.put::<T>(_key, _value)?;
        self.iter.set_mode(rocksdb::IteratorMode::From(&ext_key, cursor_direction(&self.current)));
        self.current = CursorIt::Key(ext_key.into(), cursor_direction(&self.current));
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
                let ext_key = _key.clone().encode();
                if _item.0.encode().as_ref() > ext_key.as_ref() {
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
        match &self.current {
            CursorIt::Start(_) => panic!("should never happen"),
            CursorIt::End(_) => panic!("should never happen"),
            CursorIt::Key(ck, _) => {
                let locked_opt_tx = self.tx.inner.lock().unwrap();
                let tx = locked_opt_tx.as_ref().unwrap();
                let cf_handle = self.tx.db.cf_handle(&String::from(T::NAME)).unwrap();

                // TODO: what should happen to self.current?
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
        self.iter.set_mode(rocksdb::IteratorMode::End); // TODO: should be from current if set
        self.current = CursorIt::Start(Reverse);

        let encoded_key = _key.clone().encode();
        let _ = self.iter.find(|res| match res {
            Err(_e) => false,
            Ok((k, _v)) => {
                encoded_key.as_ref() >= k.as_ref().split_at(encoded_key.as_ref().len()).0
            }
        });

        let last_subkey_it = self.iter.next();

        match last_subkey_it {
            None => self.upsert(_key, _value),
            Some(Err(e)) => Err(DatabaseWriteError {
                info: DatabaseErrorInfo { message: e.into_string(), code: 1 },
                operation: DatabaseWriteOperation::CursorAppendDup,
                table_name: T::NAME,
                key: _key.encode().into(),
            }
            .into()),
            Some(Ok(last_subkey_el)) => {
                // Check if same primary, if not just append
                let ls_el_primary = <T as KeyFormat<T::Key, T::Value>>::unformat_key(
                    last_subkey_el.0.clone().into_vec(),
                );
                if ls_el_primary != _key {
                    return self.upsert(_key, _value);
                }

                let ext_key =
                    <T as KeyFormat<T::Key, T::Value>>::format_key(_key.clone(), &_value).into();
                if last_subkey_el.0 > ext_key {
                    return Err(DatabaseWriteError {
                        info: DatabaseErrorInfo { message: "KeyMismatch".into(), code: 1 },
                        operation: DatabaseWriteOperation::CursorAppendDup,
                        table_name: T::NAME,
                        key: _key.encode().into(),
                    }
                    .into());
                }
                return self.upsert(_key, _value);
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
