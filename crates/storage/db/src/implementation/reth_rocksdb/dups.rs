use crate::{
    table::{Decode, Encode, KeyFormat, Table},
    tables::{self},
};

use reth_primitives::BlockNumber;

impl
    KeyFormat<
        <tables::PlainStorageState as Table>::Key,
        <tables::PlainStorageState as Table>::Value,
    > for tables::PlainStorageState
{
    fn format_key(
        k: <tables::PlainStorageState as Table>::Key,
        v: &<tables::PlainStorageState as Table>::Value,
    ) -> Vec<u8> {
        let mut primary_key: Vec<u8> = k.encode().as_ref().into();
        primary_key.extend_from_slice(v.key.as_slice());
        primary_key
    }

    fn unformat_key(raw_key: Vec<u8>) -> <tables::PlainStorageState as Table>::Key {
        println!("unformat raw key {:?}", raw_key);
        <tables::PlainStorageState as Table>::Key::decode(raw_key.as_slice().split_at(20).0)
            .unwrap()
    }
}

impl
    KeyFormat<
        <tables::AccountChangeSets as Table>::Key,
        <tables::AccountChangeSets as Table>::Value,
    > for tables::AccountChangeSets
{
    fn format_key(
        k: <tables::AccountChangeSets as Table>::Key,
        v: &<tables::AccountChangeSets as Table>::Value,
    ) -> Vec<u8> {
        let mut primary_key: Vec<u8> = k.encode().as_ref().into();
        primary_key.extend_from_slice(&v.address.into_array());
        primary_key
    }

    fn unformat_key(raw_key: Vec<u8>) -> <tables::AccountChangeSets as Table>::Key {
        println!("unformat raw key {:?}", raw_key);
        BlockNumber::decode(raw_key.as_slice().split_at(8).0).unwrap()
    }
}

impl
    KeyFormat<
        <tables::StorageChangeSets as Table>::Key,
        <tables::StorageChangeSets as Table>::Value,
    > for tables::StorageChangeSets
{
    fn format_key(
        k: <tables::StorageChangeSets as Table>::Key,
        _v: &<tables::StorageChangeSets as Table>::Value,
    ) -> Vec<u8> {
        let primary_key: Vec<u8> = k.encode().as_ref().into();
        primary_key
    }

    fn unformat_key(raw_key: Vec<u8>) -> <tables::StorageChangeSets as Table>::Key {
        <tables::StorageChangeSets as Table>::Key::decode(raw_key).unwrap()
    }
}

impl KeyFormat<<tables::HashedStorages as Table>::Key, <tables::HashedStorages as Table>::Value>
    for tables::HashedStorages
{
    fn format_key(
        k: <tables::HashedStorages as Table>::Key,
        _v: &<tables::HashedStorages as Table>::Value,
    ) -> Vec<u8> {
        let primary_key: Vec<u8> = k.encode().as_ref().into();
        primary_key
    }

    fn unformat_key(raw_key: Vec<u8>) -> <tables::HashedStorages as Table>::Key {
        <tables::HashedStorages as Table>::Key::decode(raw_key).unwrap()
    }
}

impl KeyFormat<<tables::StoragesTrie as Table>::Key, <tables::StoragesTrie as Table>::Value>
    for tables::StoragesTrie
{
    fn format_key(
        k: <tables::HashedStorages as Table>::Key,
        _v: &<tables::StoragesTrie as Table>::Value,
    ) -> Vec<u8> {
        let primary_key: Vec<u8> = k.encode().as_ref().into();
        primary_key
    }

    fn unformat_key(raw_key: Vec<u8>) -> <tables::StoragesTrie as Table>::Key {
        <tables::StoragesTrie as Table>::Key::decode(raw_key).unwrap()
    }
}

/*
    table PlainStorageState<Key = Address, Value = StorageEntry, SubKey = B256>;
    table AccountChangeSets<Key = BlockNumber, Value = AccountBeforeTx, SubKey = Address>;
    table StorageChangeSets<Key = BlockNumberAddress, Value = StorageEntry, SubKey = B256>;
    table HashedStorages<Key = B256, Value = StorageEntry, SubKey = B256>;
    table StoragesTrie<Key = B256, Value = StorageTrieEntry, SubKey = StoredNibblesSubKey>;
*/
