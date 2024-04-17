use crate::{
    table::{Decode, DupKeyFormat, DupSort, Encode, KeyFormat, Table},
    tables,
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
        <tables::PlainStorageState as DupKeyFormat<
            <tables::PlainStorageState as Table>::Key,
            <tables::PlainStorageState as DupSort>::SubKey,
        >>::format_composite_key(k, v.key.clone())
    }

    fn unformat_key(raw_key: &[u8]) -> <tables::PlainStorageState as Table>::Key {
        <tables::PlainStorageState as Table>::Key::decode(raw_key.split_at(20).0).unwrap()
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
        <tables::AccountChangeSets as DupKeyFormat<
            <tables::AccountChangeSets as Table>::Key,
            <tables::AccountChangeSets as DupSort>::SubKey,
        >>::format_composite_key(k, v.address.clone())
    }

    fn unformat_key(raw_key: &[u8]) -> <tables::AccountChangeSets as Table>::Key {
        BlockNumber::decode(raw_key.split_at(8).0).unwrap()
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
        v: &<tables::StorageChangeSets as Table>::Value,
    ) -> Vec<u8> {
        <tables::StorageChangeSets as DupKeyFormat<
            <tables::StorageChangeSets as Table>::Key,
            <tables::StorageChangeSets as DupSort>::SubKey,
        >>::format_composite_key(k, v.key.clone())
    }

    fn unformat_key(raw_key: &[u8]) -> <tables::StorageChangeSets as Table>::Key {
        crate::tables::models::accounts::BlockNumberAddress::decode(raw_key.split_at(28).0).unwrap()
    }
}

impl KeyFormat<<tables::HashedStorages as Table>::Key, <tables::HashedStorages as Table>::Value>
    for tables::HashedStorages
{
    fn format_key(
        k: <tables::HashedStorages as Table>::Key,
        v: &<tables::HashedStorages as Table>::Value,
    ) -> Vec<u8> {
        <tables::HashedStorages as DupKeyFormat<
            <tables::HashedStorages as Table>::Key,
            <tables::HashedStorages as DupSort>::SubKey,
        >>::format_composite_key(k, v.key.clone())
    }

    fn unformat_key(raw_key: &[u8]) -> <tables::HashedStorages as Table>::Key {
        <tables::HashedStorages as Table>::Key::decode(raw_key.split_at(32).0).unwrap()
    }
}

impl KeyFormat<<tables::StoragesTrie as Table>::Key, <tables::StoragesTrie as Table>::Value>
    for tables::StoragesTrie
{
    fn format_key(
        k: <tables::StoragesTrie as Table>::Key,
        v: &<tables::StoragesTrie as Table>::Value,
    ) -> Vec<u8> {
        <tables::StoragesTrie as DupKeyFormat<
            <tables::StoragesTrie as Table>::Key,
            <tables::StoragesTrie as DupSort>::SubKey,
        >>::format_composite_key(k, v.nibbles.clone())
    }

    fn unformat_key(raw_key: &[u8]) -> <tables::StoragesTrie as Table>::Key {
        <tables::StoragesTrie as Table>::Key::decode(raw_key.split_at(32).0).unwrap()
    }
}

/*
    table PlainStorageState<Key = Address, Value = StorageEntry, SubKey = B256>;
    table AccountChangeSets<Key = BlockNumber, Value = AccountBeforeTx, SubKey = Address>;
    table StorageChangeSets<Key = BlockNumberAddress, Value = StorageEntry, SubKey = B256>;
    table HashedStorages<Key = B256, Value = StorageEntry, SubKey = B256>;
    table StoragesTrie<Key = B256, Value = StorageTrieEntry, SubKey = StoredNibblesSubKey>;
*/
