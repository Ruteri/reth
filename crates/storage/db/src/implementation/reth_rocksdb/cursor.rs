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

use std::ops::RangeBounds;
use std::{borrow::BorrowMut, fmt};
use std::{borrow::Cow, iter::Rev};

use rocksdb;

use rocksdb::Direction::{self, Forward, Reverse};

#[derive(Clone)]
enum CursorIt {
    Start,
    End,
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
    // TODO: use peekable once it's not unstable
    table_type: std::marker::PhantomData<T>,
}

impl<'itx, 'it: 'itx, T: Table> Cursor<'itx, 'it, T> {
    pub fn new(
        mut iter: rocksdb::DBRawIteratorWithThreadMode<
            'it,
            rocksdb::Transaction<'_, rocksdb::TransactionDB>,
        >,
        tx: &'itx reth_rocksdb::tx::Tx<'it, rocksdb::TransactionDB>,
    ) -> Cursor<'itx, 'it, T> {
        Self { iter, tx, state: CursorIt::Start, table_type: std::marker::PhantomData }
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
        let encoded_key = _key.clone().encode();
        self.iter.seek(encoded_key.as_ref());
        match self.iter.item() {
            None => {
                self.state = CursorIt::End;
                Ok(None)
            }
            Some(el) => {
                self.state = CursorIt::Iterating;
                match encoded_key.as_ref() == el.0.as_ref().split_at(encoded_key.as_ref().len()).0 {
                    true => decode_item::<T>(Some(el)),
                    false => Ok(None),
                }
            }
        }
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
            CursorIt::End => Ok(None),
            CursorIt::Iterating => {
                self.iter.next();
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
        }
    }

    fn prev(&mut self) -> PairResult<T> {
        match self.state {
            CursorIt::Start => self.last(),
            CursorIt::End => self.last(),
            CursorIt::Iterating => {
                self.iter.prev();
                match self.iter.item() {
                    None => {
                        self.state = CursorIt::Start;
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
                    let prev_primary =
                        <T as KeyFormat<T::Key, T::Value>>::unformat_key(prev_item.0.to_vec());
                    self.iter.next();
                    match self.iter.item() {
                        None => {
                            self.state = CursorIt::End;
                            Ok(None)
                        }
                        Some(el) => {
                            self.state = CursorIt::Iterating;
                            let el_primary = <T as KeyFormat<T::Key, T::Value>>::unformat_key(
                                el.0.clone().to_vec(),
                            );
                            if prev_primary == el_primary {
                                decode_item::<T>(Some(el))
                            } else {
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

                let mut prev_primary_plus_one: Vec<u8> =
                    <T as KeyFormat<T::Key, T::Value>>::unformat_key(prev_item.unwrap().0.to_vec())
                        .encode()
                        .into();
                for i in prev_primary_plus_one.len()..1 {
                    if prev_primary_plus_one[i - 1] != u8::max_value() {
                        prev_primary_plus_one[i - 1] = prev_primary_plus_one[i - 1] + 1;
                    }
                }

                self.iter.seek(prev_primary_plus_one);
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
        let ext_key = T::format_composite_key(_key.clone(), _subkey.clone());
        self.iter.seek(&ext_key);

        match self.iter.item() {
            None => {
                self.state = CursorIt::End;
                Ok(None)
            }
            Some(el) => {
                self.state = CursorIt::Iterating;
                let encoded_key = _key.encode();
                if encoded_key.as_ref() != el.0.as_ref().split_at(encoded_key.as_ref().len()).0 {
                    return Ok(None);
                }
                decode_value::<T>(el.1)
            }
        }
    }

    fn walk_dup(
        &mut self,
        _key: Option<<T>::Key>,
        _subkey: Option<<T as DupSort>::SubKey>,
    ) -> Result<DupWalker<'_, T, Self>, DatabaseError> {
        let start_el = match (_key, _subkey) {
            (None, None) => self.first(),
            (None, Some(subkey)) => {
                panic!("not implemented");
            }
            (Some(key), None) => self.seek(key),
            (Some(key), Some(subkey)) => {
                let ext_key = T::format_composite_key(key.clone(), subkey.clone().into());
                self.iter.seek(&ext_key);
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
        self.iter.seek(&ext_key);
        match self.iter.item() {
            None => {
                panic!("could not find item we just put");
                self.state = CursorIt::End;
            }
            Some(el) => {
                self.state = CursorIt::Iterating;
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
        match self.state {
            CursorIt::Start => Ok(()),
            CursorIt::End => Ok(()),
            CursorIt::Iterating => {
                match self.iter.key() {
                    None => Ok(()),
                    Some(key) => {
                        let locked_opt_tx = self.tx.inner.lock().unwrap();
                        let tx = locked_opt_tx.as_ref().unwrap();
                        let cf_handle = self.tx.db.cf_handle(&String::from(T::NAME)).unwrap();

                        // TODO: what should happen to self.current?
                        let _ = tx.delete_cf(cf_handle, &key);
                        self.iter.seek(key.to_vec().as_slice());
                        match self.iter.item() {
                            None => {
                                self.state = CursorIt::End;
                            }
                            Some(_) => {
                                self.state = CursorIt::Iterating;
                            }
                        }
                        Ok(())
                    }
                }
            }
        }
    }
}

impl<T: DupSort> DbDupCursorRW<T> for Cursor<'_, '_, T> {
    fn delete_current_duplicates(&mut self) -> Result<(), DatabaseError> {
        Ok(()) // NOOP in rocksdb
    }

    fn append_dup(&mut self, _key: <T>::Key, _value: <T>::Value) -> Result<(), DatabaseError> {
        let mut prev_primary_plus_one: Vec<u8> = _key.clone().encode().as_ref().to_vec();
        for i in prev_primary_plus_one.len()..1 {
            if prev_primary_plus_one[i - 1] != u8::max_value() {
                prev_primary_plus_one[i - 1] = prev_primary_plus_one[i - 1] + 1;
            }
        }

        self.iter.seek(prev_primary_plus_one);

        // upsert sets self.state
        match self.iter.item() {
            None => self.upsert(_key, _value),
            Some(el) => Err(DatabaseWriteError {
                info: DatabaseErrorInfo { message: "KeyMismatch".into(), code: 1 },
                operation: DatabaseWriteOperation::CursorAppendDup,
                table_name: T::NAME,
                key: _key.encode().into(),
            }
            .into()),
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
            let key = <T as KeyFormat<T::Key, T::Value>>::unformat_key(el.0.to_vec());
            let value = decode_one::<T>(Cow::Owned(el.1.to_vec())).map_err(|e| {
                DatabaseError::Read(DatabaseErrorInfo { message: e.to_string(), code: 1 })
            })?;
            Ok(Some((key, value)))
        }
    }
}

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

pub fn decode_value<T>(v: &[u8]) -> Result<Option<T::Value>, DatabaseError>
where
    T: Table,
    T::Key: Decode,
    T::Value: Decompress,
{
    Some(decode_one::<T>(Cow::Owned(v.to_vec()))).transpose()
}
