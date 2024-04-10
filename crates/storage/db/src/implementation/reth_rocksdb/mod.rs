//! Module that interacts with MDBX.

use crate::{
    cursor::{DbCursorRO, DbCursorRW},
    database::Database,
    database_metrics::{DatabaseMetadata, DatabaseMetadataValue, DatabaseMetrics},
    metrics::DatabaseEnvMetrics,
    models::client_version::ClientVersion,
    tables::Tables,
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};
use metrics::{gauge, Label};
use reth_interfaces::db::{DatabaseErrorInfo, LogLevel};

#[derive(Debug)]
#[non_exhaustive]
pub struct RO;

#[derive(Debug)]
#[non_exhaustive]
pub struct RW;

use std::{fmt, path::Path, sync::Arc, time::Duration};

pub mod cursor;
pub mod dups;
pub mod tx;

use tx::Tx;

/// Environment used when opening a MDBX environment. RO/RW.
#[derive(Debug)]
pub enum DatabaseEnvKind {
    RO,
    RW,
}

impl DatabaseEnvKind {
    /// Returns `true` if the environment is read-write.
    pub fn is_rw(&self) -> bool {
        matches!(self, Self::RW)
    }
}

/// Arguments for database initialization.
#[derive(Clone, Debug)]
pub struct DatabaseArguments {
    /// Client version that accesses the database.
    client_version: ClientVersion,
    /// Database log level. If [None], the default value is used.
    log_level: Option<LogLevel>,
}

impl DatabaseArguments {
    // See rocksdb.Options
    /// Create new database arguments with given client version.
    pub fn new(client_version: ClientVersion) -> Self {
        Self { client_version, log_level: None }
    }

    /// Set the log level.
    pub fn with_log_level(mut self, log_level: Option<LogLevel>) -> Self {
        self.log_level = log_level;
        self
    }

    /// Set the maximum duration of a read transaction.
    pub fn with_max_read_transaction_duration(
        self,
        _max_read_transaction_duration: Option<MaxReadTransactionDuration>,
    ) -> Self {
        self
    }

    /// Set the mdbx exclusive flag.
    pub fn with_exclusive(self, _exclusive: Option<bool>) -> Self {
        self
    }

    /// Returns the client version if any.
    pub fn client_version(&self) -> &ClientVersion {
        &self.client_version
    }
}

/// Wrapper for the libmdbx environment: [Environment]
pub struct DatabaseEnv {
    inner: rocksdb::TransactionDB,
    /// Cache for metric handles. If `None`, metrics are not recorded.
    metrics: Option<Arc<DatabaseEnvMetrics>>,
}

impl fmt::Debug for DatabaseEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatabaseEnv").finish()
    }
}

impl<'itx> Database for DatabaseEnv {
    type TX = tx::Tx<'static, rocksdb::TransactionDB>;
    type TXMut = tx::Tx<'static, rocksdb::TransactionDB>;

    // Database::TX is required to be 'static, and the only way that is possible is with unsafe
    // Requires refactoring Database trait which should never have required 'static transactions
    fn tx(&self) -> Result<Self::TX, DatabaseError> {
        let static_db = (|| -> &'static rocksdb::TransactionDB {
            let db = &self.inner as *const rocksdb::TransactionDB;
            unsafe { &*db }
        })();

        let static_tx: rocksdb::Transaction<'static, rocksdb::TransactionDB> =
            static_db.transaction();
        Ok(Tx::new(static_tx, static_db))
    }

    // Database::TXMut is required to be 'static, and the only way that is possible is with unsafe
    // Requires refactoring Database trait which should never have required 'static transactions
    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError> {
        let static_db = (|| -> &'static rocksdb::TransactionDB {
            let db = &self.inner as *const rocksdb::TransactionDB;
            unsafe { &*db }
        })();

        let static_tx: rocksdb::Transaction<'static, rocksdb::TransactionDB> =
            static_db.transaction();
        Ok(Tx::new(static_tx, static_db))
    }
}

impl DatabaseMetrics for DatabaseEnv {
    fn report_metrics(&self) {
        for (name, value, labels) in self.gauge_metrics() {
            gauge!(name, value, labels);
        }
    }

    fn gauge_metrics(&self) -> Vec<(&'static str, f64, Vec<Label>)> {
        let metrics = Vec::new();

        // See mdbx implementation

        metrics
    }
}

impl DatabaseMetadata for DatabaseEnv {
    fn metadata(&self) -> DatabaseMetadataValue {
        DatabaseMetadataValue::default()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum MaxReadTransactionDuration {
    /// The maximum duration of a read transaction is unbounded.
    Unbounded,
    /// The maximum duration of a read transaction is set to the given duration.
    Set(Duration),
}

impl MaxReadTransactionDuration {
    pub fn as_duration(&self) -> Option<Duration> {
        match self {
            MaxReadTransactionDuration::Unbounded => None,
            MaxReadTransactionDuration::Set(duration) => Some(*duration),
        }
    }
}

impl DatabaseEnv {
    /// Opens the database at the specified path with the given `EnvKind`.
    ///
    /// It does not create the tables, for that call [`DatabaseEnv::create_tables`].
    pub fn open(
        path: &Path,
        _kind: DatabaseEnvKind,
        _args: DatabaseArguments,
    ) -> Result<DatabaseEnv, DatabaseError> {
        let mut opts = rocksdb::Options::default();
        opts.enable_statistics();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let tx_opts = rocksdb::TransactionDBOptions::default();

        if let Ok(mut inner) =
            rocksdb::TransactionDB::<rocksdb::SingleThreaded>::open(&opts, &tx_opts, path)
        {
            for table in Tables::ALL {
                inner.create_cf(table.name(), &rocksdb::Options::default()).map_err(|e| {
                    DatabaseError::CreateTable(DatabaseErrorInfo {
                        message: e.to_string(),
                        code: 1,
                    })
                })?;
            }
            return Ok(DatabaseEnv { inner, metrics: None });
        }

        let mut cfs: Vec<rocksdb::ColumnFamilyDescriptor> = Vec::new();
        for table in Tables::ALL {
            cfs.push(rocksdb::ColumnFamilyDescriptor::new(
                table.name(),
                rocksdb::Options::default(),
            ))
        }

        let inner =
            rocksdb::TransactionDB::open_cf_descriptors(&opts, &tx_opts, path, cfs).unwrap();
        Ok(DatabaseEnv { inner, metrics: None })
    }

    /// Enables metrics on the database.
    pub fn with_metrics(mut self) -> Self {
        self.metrics = Some(DatabaseEnvMetrics::new().into());
        self
    }

    /// Creates all the defined tables, if necessary.
    pub fn create_tables(&mut self) -> Result<(), DatabaseError> {
        Ok(())
    }

    /// Records version that accesses the database with write privileges.
    pub fn record_client_version(&self, _version: ClientVersion) -> Result<(), DatabaseError> {
        /*
        if version.is_empty() {
            return Ok(());
        }

        let tx = self.tx_mut()?;
        let mut version_cursor = tx.cursor_write::<tables::VersionHistory>()?;

        let last_version = version_cursor.last()?.map(|(_, v)| v);
        if Some(&version) != last_version.as_ref() {
            version_cursor.upsert(
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
                version,
            )?;
            tx.commit()?;
        }
        */

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        abstraction::table::{Encode, Table},
        cursor::{DbDupCursorRO, DbDupCursorRW, ReverseWalker, Walker},
        models::{AccountBeforeTx, ShardedKey},
        tables::{
            AccountsHistory, CanonicalHeaders, Headers, PlainAccountState, PlainStorageState,
        },
        test_utils::*,
        AccountChangeSets,
    };
    use reth_interfaces::db::{DatabaseWriteError, DatabaseWriteOperation};
    use reth_primitives::{Account, Address, Header, IntegerList, StorageEntry, B256, U256};
    use std::str::FromStr;

    /// Create database for testing
    fn create_test_db(kind: DatabaseEnvKind) -> Arc<DatabaseEnv> {
        Arc::new(create_test_db_with_path(
            kind,
            &tempfile::TempDir::new().expect(ERROR_TEMPDIR).into_path(),
        ))
    }

    /// Create database for testing with specified path
    fn create_test_db_with_path(kind: DatabaseEnvKind, path: &Path) -> DatabaseEnv {
        let mut env =
            DatabaseEnv::open(path, kind, DatabaseArguments::new(ClientVersion::default()))
                .expect(ERROR_DB_CREATION);
        env.create_tables().expect(ERROR_TABLE_CREATION);
        env
    }

    const ERROR_DB_CREATION: &str = "Not able to create the mdbx file.";
    const ERROR_PUT: &str = "Not able to insert value into table.";
    const ERROR_APPEND: &str = "Not able to append the value to the table.";
    const ERROR_UPSERT: &str = "Not able to upsert the value to the table.";
    const ERROR_GET: &str = "Not able to get value from table.";
    const ERROR_COMMIT: &str = "Not able to commit transaction.";
    const ERROR_RETURN_VALUE: &str = "Mismatching result.";
    const ERROR_INIT_TX: &str = "Failed to create a MDBX transaction.";
    const ERROR_ETH_ADDRESS: &str = "Invalid address.";

    #[test]
    fn db_creation() {
        create_test_db(DatabaseEnvKind::RW);
    }

    #[test]
    fn db_manual_put_get() {
        let env = create_test_db(DatabaseEnvKind::RW);

        let value = Header::default();
        let key = 1u64;

        // PUT
        let tx = env.tx_mut().expect(ERROR_INIT_TX);
        tx.put::<Headers>(key, value.clone()).expect(ERROR_PUT);
        tx.commit().expect(ERROR_COMMIT);

        // GET
        let tx = env.tx().expect(ERROR_INIT_TX);
        let result = tx.get::<Headers>(key).expect(ERROR_GET).expect(ERROR_RETURN_VALUE);
        assert!(result == value);
        tx.commit().expect(ERROR_COMMIT);
    }

    #[test]
    fn db_manual_put_get_dup() {
        let env = create_test_db(DatabaseEnvKind::RW);

        let address0 = Address::ZERO;
        let address1 = Address::with_last_byte(1);

        let tx = env.tx_mut().expect(ERROR_INIT_TX);
        tx.put::<AccountChangeSets>(1, AccountBeforeTx { address: address0, info: None })
            .expect(ERROR_PUT);
        tx.put::<AccountChangeSets>(1, AccountBeforeTx { address: address1, info: None })
            .expect(ERROR_PUT);
        tx.put::<AccountChangeSets>(2, AccountBeforeTx { address: address1, info: None })
            .expect(ERROR_PUT);
        tx.commit().expect(ERROR_COMMIT);

        // GET
        let tx = env.tx().expect(ERROR_INIT_TX);
        let result = tx.get::<AccountChangeSets>(1).expect(ERROR_GET).expect(ERROR_RETURN_VALUE);
        assert!(result == AccountBeforeTx { address: address0, info: None });
        tx.commit().expect(ERROR_COMMIT);

        let tx = env.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_read::<AccountChangeSets>().unwrap();
        let mut walker = cursor.walk_range(0..=1).unwrap();
        assert_eq!(walker.next(), Some(Ok((1, AccountBeforeTx { address: address0, info: None }))));
        assert_eq!(walker.next(), Some(Ok((1, AccountBeforeTx { address: address1, info: None }))));
        assert_eq!(walker.next(), None,);

        let mut cursor = tx.cursor_read::<AccountChangeSets>().unwrap();

        assert_eq!(
            cursor.seek_exact(2),
            Ok(Some((2, AccountBeforeTx { address: address1, info: None })))
        );
    }

    #[test]
    fn db_cursor_walk() {
        let env = create_test_db(DatabaseEnvKind::RW);

        let value = Header::default();
        let key = 1u64;

        // PUT
        let tx = env.tx_mut().expect(ERROR_INIT_TX);
        tx.put::<Headers>(key, value.clone()).expect(ERROR_PUT);
        tx.commit().expect(ERROR_COMMIT);

        // Cursor
        let tx = env.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_read::<Headers>().unwrap();

        let first = cursor.first();
        assert!(first.unwrap().is_some(), "First should be our put");

        // Walk
        let walk = cursor.walk(Some(key)).unwrap();
        let first = walk.into_iter().next().unwrap().unwrap();
        assert_eq!(first.1, value, "First next should be put value");
    }

    #[test]
    fn db_cursor_walk_range() {
        let db: Arc<DatabaseEnv> = create_test_db(DatabaseEnvKind::RW);

        // PUT (0, 0), (1, 0), (2, 0), (3, 0)
        let tx = db.tx_mut().expect(ERROR_INIT_TX);
        vec![0, 1, 2, 3]
            .into_iter()
            .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
            .expect(ERROR_PUT);
        tx.commit().expect(ERROR_COMMIT);

        let tx = db.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();

        // [1, 3)
        let mut walker = cursor.walk_range(1..3).unwrap();
        assert_eq!(walker.next(), Some(Ok((1, B256::ZERO))));
        assert_eq!(walker.next(), Some(Ok((2, B256::ZERO))));
        assert_eq!(walker.next(), None);
        // next() returns None after walker is done
        assert_eq!(walker.next(), None);

        // [1, 2]
        let mut walker = cursor.walk_range(1..=2).unwrap();
        assert_eq!(walker.next(), Some(Ok((1, B256::ZERO))));
        assert_eq!(walker.next(), Some(Ok((2, B256::ZERO))));
        // next() returns None after walker is done
        assert_eq!(walker.next(), None);

        // [1, ∞)
        let mut walker = cursor.walk_range(1..).unwrap();
        assert_eq!(walker.next(), Some(Ok((1, B256::ZERO))));
        assert_eq!(walker.next(), Some(Ok((2, B256::ZERO))));
        assert_eq!(walker.next(), Some(Ok((3, B256::ZERO))));
        // next() returns None after walker is done
        assert_eq!(walker.next(), None);

        // [2, 4)
        let mut walker = cursor.walk_range(2..4).unwrap();
        assert_eq!(walker.next(), Some(Ok((2, B256::ZERO))));
        assert_eq!(walker.next(), Some(Ok((3, B256::ZERO))));
        assert_eq!(walker.next(), None);
        // next() returns None after walker is done
        assert_eq!(walker.next(), None);

        // (∞, 3)
        let mut walker = cursor.walk_range(..3).unwrap();
        assert_eq!(walker.next(), Some(Ok((0, B256::ZERO))));
        assert_eq!(walker.next(), Some(Ok((1, B256::ZERO))));
        assert_eq!(walker.next(), Some(Ok((2, B256::ZERO))));
        // next() returns None after walker is done
        assert_eq!(walker.next(), None);

        // (∞, ∞)
        let mut walker = cursor.walk_range(..).unwrap();
        assert_eq!(walker.next(), Some(Ok((0, B256::ZERO))));
        assert_eq!(walker.next(), Some(Ok((1, B256::ZERO))));
        assert_eq!(walker.next(), Some(Ok((2, B256::ZERO))));
        assert_eq!(walker.next(), Some(Ok((3, B256::ZERO))));
        // next() returns None after walker is done
        assert_eq!(walker.next(), None);
    }

    #[test]
    fn db_cursor_walk_range_on_dup_table() {
        let db: Arc<DatabaseEnv> = create_test_db(DatabaseEnvKind::RW);

        let address0 = Address::ZERO;
        let address1 = Address::with_last_byte(1);
        let address2 = Address::with_last_byte(2);

        let tx = db.tx_mut().expect(ERROR_INIT_TX);
        tx.put::<AccountChangeSets>(0, AccountBeforeTx { address: address0, info: None })
            .expect(ERROR_PUT);
        tx.put::<AccountChangeSets>(0, AccountBeforeTx { address: address1, info: None })
            .expect(ERROR_PUT);
        tx.put::<AccountChangeSets>(0, AccountBeforeTx { address: address2, info: None })
            .expect(ERROR_PUT);
        tx.put::<AccountChangeSets>(1, AccountBeforeTx { address: address0, info: None })
            .expect(ERROR_PUT);
        tx.put::<AccountChangeSets>(1, AccountBeforeTx { address: address1, info: None })
            .expect(ERROR_PUT);
        tx.put::<AccountChangeSets>(1, AccountBeforeTx { address: address2, info: None })
            .expect(ERROR_PUT);
        tx.put::<AccountChangeSets>(2, AccountBeforeTx { address: address0, info: None }) // <- should not be returned by the walker
            .expect(ERROR_PUT);
        tx.commit().expect(ERROR_COMMIT);

        let tx = db.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_read::<AccountChangeSets>().unwrap();

        let walker = cursor.walk_range(..).unwrap();

        let entries = walker.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(entries.len(), 7);

        let mut walker = cursor.walk_range(0..=1).unwrap();
        assert_eq!(walker.next(), Some(Ok((0, AccountBeforeTx { address: address0, info: None }))));
        assert_eq!(
            walker.next(),
            Some(Ok((0u64, AccountBeforeTx { address: address1, info: None })))
        );
        assert_eq!(
            walker.next(),
            Some(Ok((0u64, AccountBeforeTx { address: address2, info: None })))
        );
        assert_eq!(
            walker.next(),
            Some(Ok((1u64, AccountBeforeTx { address: address0, info: None })))
        );
        assert_eq!(
            walker.next(),
            Some(Ok((1u64, AccountBeforeTx { address: address1, info: None })))
        );
        assert_eq!(
            walker.next(),
            Some(Ok((1u64, AccountBeforeTx { address: address2, info: None })))
        );
        assert_eq!(walker.next(), None);
    }

    #[allow(clippy::reversed_empty_ranges)]
    #[test]
    fn db_cursor_walk_range_invalid() {
        let db: Arc<DatabaseEnv> = create_test_db(DatabaseEnvKind::RW);

        // PUT (0, 0), (1, 0), (2, 0), (3, 0)
        let tx = db.tx_mut().expect(ERROR_INIT_TX);
        vec![0, 1, 2, 3]
            .into_iter()
            .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
            .expect(ERROR_PUT);
        tx.commit().expect(ERROR_COMMIT);

        let tx = db.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();

        // start bound greater than end bound
        let mut res = cursor.walk_range(3..1).unwrap();
        assert_eq!(res.next(), None);

        // start bound greater than end bound
        res = cursor.walk_range(15..=2).unwrap();
        assert_eq!(res.next(), None);

        // returning nothing
        let mut walker = cursor.walk_range(1..1).unwrap();
        assert_eq!(walker.next(), None);
    }

    #[test]
    fn db_walker() {
        let db: Arc<DatabaseEnv> = create_test_db(DatabaseEnvKind::RW);

        // PUT (0, 0), (1, 0), (3, 0)
        let tx = db.tx_mut().expect(ERROR_INIT_TX);
        vec![0, 1, 3]
            .into_iter()
            .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
            .expect(ERROR_PUT);
        tx.commit().expect(ERROR_COMMIT);

        let tx = db.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();

        let mut walker = Walker::new(&mut cursor, None);

        assert_eq!(walker.next(), Some(Ok((0, B256::ZERO))));
        assert_eq!(walker.next(), Some(Ok((1, B256::ZERO))));
        assert_eq!(walker.next(), Some(Ok((3, B256::ZERO))));
        assert_eq!(walker.next(), None);

        // transform to ReverseWalker
        let mut reverse_walker = walker.rev();
        assert_eq!(reverse_walker.next(), Some(Ok((3, B256::ZERO))));
        assert_eq!(reverse_walker.next(), Some(Ok((1, B256::ZERO))));
        assert_eq!(reverse_walker.next(), Some(Ok((0, B256::ZERO))));
        assert_eq!(reverse_walker.next(), None);
    }

    #[test]
    fn db_reverse_walker() {
        let db: Arc<DatabaseEnv> = create_test_db(DatabaseEnvKind::RW);

        // PUT (0, 0), (1, 0), (3, 0)
        let tx = db.tx_mut().expect(ERROR_INIT_TX);
        vec![0, 1, 3]
            .into_iter()
            .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
            .expect(ERROR_PUT);
        tx.commit().expect(ERROR_COMMIT);

        let tx = db.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();

        let mut reverse_walker = ReverseWalker::new(&mut cursor, None);

        assert_eq!(reverse_walker.next(), Some(Ok((3, B256::ZERO))));
        assert_eq!(reverse_walker.next(), Some(Ok((1, B256::ZERO))));
        assert_eq!(reverse_walker.next(), Some(Ok((0, B256::ZERO))));
        assert_eq!(reverse_walker.next(), None);

        // transform to Walker
        let mut walker = reverse_walker.forward();
        assert_eq!(walker.next(), Some(Ok((0, B256::ZERO))));
        assert_eq!(walker.next(), Some(Ok((1, B256::ZERO))));
        assert_eq!(walker.next(), Some(Ok((3, B256::ZERO))));
        assert_eq!(walker.next(), None);
    }

    #[test]
    fn db_walk_back() {
        let db: Arc<DatabaseEnv> = create_test_db(DatabaseEnvKind::RW);

        // PUT (0, 0), (1, 0), (3, 0)
        let tx = db.tx_mut().expect(ERROR_INIT_TX);
        vec![0, 1, 3]
            .into_iter()
            .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
            .expect(ERROR_PUT);
        tx.commit().expect(ERROR_COMMIT);

        let tx = db.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();

        let mut reverse_walker = cursor.walk_back(Some(1)).unwrap();
        assert_eq!(reverse_walker.next(), Some(Ok((1, B256::ZERO))));
        assert_eq!(reverse_walker.next(), Some(Ok((0, B256::ZERO))));
        assert_eq!(reverse_walker.next(), None);

        let mut reverse_walker = cursor.walk_back(Some(2)).unwrap();
        assert_eq!(reverse_walker.next(), Some(Ok((3, B256::ZERO))));
        assert_eq!(reverse_walker.next(), Some(Ok((1, B256::ZERO))));
        assert_eq!(reverse_walker.next(), Some(Ok((0, B256::ZERO))));
        assert_eq!(reverse_walker.next(), None);

        let mut reverse_walker = cursor.walk_back(Some(4)).unwrap();
        assert_eq!(reverse_walker.next(), Some(Ok((3, B256::ZERO))));
        assert_eq!(reverse_walker.next(), Some(Ok((1, B256::ZERO))));
        assert_eq!(reverse_walker.next(), Some(Ok((0, B256::ZERO))));
        assert_eq!(reverse_walker.next(), None);

        let mut reverse_walker = cursor.walk_back(None).unwrap();
        assert_eq!(reverse_walker.next(), Some(Ok((3, B256::ZERO))));
        assert_eq!(reverse_walker.next(), Some(Ok((1, B256::ZERO))));
        assert_eq!(reverse_walker.next(), Some(Ok((0, B256::ZERO))));
        assert_eq!(reverse_walker.next(), None);
    }

    #[test]
    fn db_cursor_seek_exact_or_previous_key() {
        let db: Arc<DatabaseEnv> = create_test_db(DatabaseEnvKind::RW);

        // PUT
        let tx = db.tx_mut().expect(ERROR_INIT_TX);
        vec![0, 1, 3]
            .into_iter()
            .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
            .expect(ERROR_PUT);
        tx.commit().expect(ERROR_COMMIT);

        // Cursor
        let missing_key = 2;
        let tx = db.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
        assert_eq!(cursor.current(), Ok(None));

        // Seek exact
        let exact = cursor.seek_exact(missing_key).unwrap();
        assert_eq!(exact, None);
        assert_eq!(cursor.current(), Ok(Some((missing_key + 1, B256::ZERO))));
        assert_eq!(cursor.prev(), Ok(Some((missing_key - 1, B256::ZERO))));
        assert_eq!(cursor.prev(), Ok(Some((missing_key - 2, B256::ZERO))));
    }

    #[test]
    fn db_cursor_insert() {
        let db: Arc<DatabaseEnv> = create_test_db(DatabaseEnvKind::RW);

        // PUT
        let tx = db.tx_mut().expect(ERROR_INIT_TX);
        vec![0, 1, 3, 4, 5]
            .into_iter()
            .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
            .expect(ERROR_PUT);
        tx.commit().expect(ERROR_COMMIT);

        let key_to_insert = 2;
        let tx = db.tx_mut().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_write::<CanonicalHeaders>().unwrap();

        // INSERT
        assert_eq!(cursor.insert(key_to_insert, B256::ZERO), Ok(()));
        assert_eq!(cursor.current(), Ok(Some((key_to_insert, B256::ZERO))));

        // INSERT (failure)
        assert_eq!(
            cursor.insert(key_to_insert, B256::ZERO),
            Err(DatabaseWriteError {
                info: DatabaseErrorInfo { message: "AlreadyExists".into(), code: 1 },
                operation: DatabaseWriteOperation::CursorInsert,
                table_name: CanonicalHeaders::NAME,
                key: key_to_insert.encode().into(),
            }
            .into())
        );
        assert_eq!(cursor.current(), Ok(Some((key_to_insert, B256::ZERO))));

        tx.commit().expect(ERROR_COMMIT);

        // Confirm the result
        let tx = db.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
        let res = cursor.walk(None).unwrap().map(|res| res.unwrap().0).collect::<Vec<_>>();
        assert_eq!(res, vec![0, 1, 2, 3, 4, 5]);
        tx.commit().expect(ERROR_COMMIT);
    }

    #[test]
    fn db_cursor_insert_dup() {
        let db: Arc<DatabaseEnv> = create_test_db(DatabaseEnvKind::RW);
        let tx = db.tx_mut().expect(ERROR_INIT_TX);

        let mut dup_cursor = tx.cursor_dup_write::<PlainStorageState>().unwrap();
        let key = Address::random();
        let subkey1 = B256::random();
        let subkey2 = B256::random();

        let entry1 = StorageEntry { key: subkey1, value: U256::ZERO };
        assert!(dup_cursor.insert(key, entry1).is_ok());

        // Can't insert
        let entry2 = StorageEntry { key: subkey2, value: U256::ZERO };
        assert!(dup_cursor.insert(key, entry2).is_err());
    }

    #[test]
    fn db_cursor_delete_current_non_existent() {
        let db: Arc<DatabaseEnv> = create_test_db(DatabaseEnvKind::RW);
        let tx = db.tx_mut().expect(ERROR_INIT_TX);

        let key1 = Address::with_last_byte(1);
        let key2 = Address::with_last_byte(2);
        let key3 = Address::with_last_byte(3);
        let mut cursor = tx.cursor_write::<PlainAccountState>().unwrap();

        assert!(cursor.insert(key1, Account::default()).is_ok());
        assert!(cursor.insert(key2, Account::default()).is_ok());
        assert!(cursor.insert(key3, Account::default()).is_ok());

        // Seek & delete key2
        cursor.seek_exact(key2).unwrap();
        assert_eq!(cursor.delete_current(), Ok(()));
        assert_eq!(cursor.seek_exact(key2), Ok(None));

        // Seek & delete key2 again
        assert_eq!(cursor.seek_exact(key2), Ok(None));
        assert_eq!(cursor.delete_current(), Ok(()));
        // Assert that key1 is still there
        assert_eq!(cursor.seek_exact(key1), Ok(Some((key1, Account::default()))));
        // Assert that key3 was deleted
        assert_eq!(cursor.seek_exact(key3), Ok(None));
    }

    #[test]
    fn db_cursor_insert_wherever_cursor_is() {
        let db: Arc<DatabaseEnv> = create_test_db(DatabaseEnvKind::RW);
        let tx = db.tx_mut().expect(ERROR_INIT_TX);

        // PUT
        vec![0, 1, 3, 5, 7, 9]
            .into_iter()
            .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
            .expect(ERROR_PUT);
        tx.commit().expect(ERROR_COMMIT);

        let tx = db.tx_mut().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_write::<CanonicalHeaders>().unwrap();

        // INSERT (cursor starts at last)
        cursor.last().unwrap();
        assert_eq!(cursor.current(), Ok(Some((9, B256::ZERO))));

        for pos in (2..=8).step_by(2) {
            assert_eq!(cursor.insert(pos, B256::ZERO), Ok(()));
            assert_eq!(cursor.current(), Ok(Some((pos, B256::ZERO))));
        }
        tx.commit().expect(ERROR_COMMIT);

        // Confirm the result
        let tx = db.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
        let res = cursor.walk(None).unwrap().map(|res| res.unwrap().0).collect::<Vec<_>>();
        assert_eq!(res, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        tx.commit().expect(ERROR_COMMIT);
    }

    #[test]
    fn db_cursor_append() {
        let db: Arc<DatabaseEnv> = create_test_db(DatabaseEnvKind::RW);

        // PUT
        let tx = db.tx_mut().expect(ERROR_INIT_TX);
        vec![0, 1, 2, 3, 4]
            .into_iter()
            .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
            .expect(ERROR_PUT);
        tx.commit().expect(ERROR_COMMIT);

        // APPEND
        let key_to_append = 5;
        let tx = db.tx_mut().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_write::<CanonicalHeaders>().unwrap();
        assert_eq!(cursor.append(key_to_append, B256::ZERO), Ok(()));
        tx.commit().expect(ERROR_COMMIT);

        // Confirm the result
        let tx = db.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
        let res = cursor.walk(None).unwrap().map(|res| res.unwrap().0).collect::<Vec<_>>();
        assert_eq!(res, vec![0, 1, 2, 3, 4, 5]);
        tx.commit().expect(ERROR_COMMIT);
    }

    #[test]
    fn db_cursor_append_failure() {
        let db: Arc<DatabaseEnv> = create_test_db(DatabaseEnvKind::RW);

        // PUT
        let tx = db.tx_mut().expect(ERROR_INIT_TX);
        vec![0, 1, 3, 4, 5]
            .into_iter()
            .try_for_each(|key| tx.put::<CanonicalHeaders>(key, B256::ZERO))
            .expect(ERROR_PUT);
        tx.commit().expect(ERROR_COMMIT);

        // APPEND
        let key_to_append = 2;
        let tx = db.tx_mut().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_write::<CanonicalHeaders>().unwrap();
        assert_eq!(
            cursor.append(key_to_append, B256::ZERO),
            Err(DatabaseWriteError {
                info: DatabaseErrorInfo { message: "KeyMismatch".into(), code: 1 },
                operation: DatabaseWriteOperation::CursorAppend,
                table_name: CanonicalHeaders::NAME,
                key: key_to_append.encode().into(),
            }
            .into())
        );
        assert_eq!(cursor.current(), Ok(Some((5, B256::ZERO)))); // the end of table
        tx.commit().expect(ERROR_COMMIT);

        // Confirm the result
        let tx = db.tx().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_read::<CanonicalHeaders>().unwrap();
        let res = cursor.walk(None).unwrap().map(|res| res.unwrap().0).collect::<Vec<_>>();
        assert_eq!(res, vec![0, 1, 3, 4, 5]);
        tx.commit().expect(ERROR_COMMIT);
    }

    #[test]
    fn db_cursor_upsert() {
        let db: Arc<DatabaseEnv> = create_test_db(DatabaseEnvKind::RW);
        let tx = db.tx_mut().expect(ERROR_INIT_TX);

        let mut cursor = tx.cursor_write::<PlainAccountState>().unwrap();
        let key = Address::random();

        let account = Account::default();
        cursor.upsert(key, account).expect(ERROR_UPSERT);
        assert_eq!(cursor.seek_exact(key), Ok(Some((key, account))));

        let account = Account { nonce: 1, ..Default::default() };
        cursor.upsert(key, account).expect(ERROR_UPSERT);
        assert_eq!(cursor.seek_exact(key), Ok(Some((key, account))));

        let account = Account { nonce: 2, ..Default::default() };
        cursor.upsert(key, account).expect(ERROR_UPSERT);
        assert_eq!(cursor.seek_exact(key), Ok(Some((key, account))));

        let mut dup_cursor = tx.cursor_dup_write::<PlainStorageState>().unwrap();
        let subkey = B256::random();

        let value = U256::from(1);
        let entry1 = StorageEntry { key: subkey, value };
        dup_cursor.upsert(key, entry1).expect(ERROR_UPSERT);
        assert_eq!(dup_cursor.seek_by_key_subkey(key, subkey), Ok(Some(entry1)));

        // TODO: this is not how upsert should work! upsert should update the exisitng (key,
        // subkey) rather than append a new value
        let value = U256::from(2);
        let entry2 = StorageEntry { key: subkey, value };
        dup_cursor.upsert(key, entry2).expect(ERROR_UPSERT);
        assert_eq!(dup_cursor.seek_by_key_subkey(key, subkey), Ok(Some(entry1)));
        assert_eq!(dup_cursor.next_dup_val(), Ok(Some(entry2)));
    }

    #[test]
    fn db_cursor_dupsort_append() {
        let db: Arc<DatabaseEnv> = create_test_db(DatabaseEnvKind::RW);

        let transition_id = 2;

        let tx = db.tx_mut().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_write::<AccountChangeSets>().unwrap();
        vec![0, 1, 3, 4, 5]
            .into_iter()
            .try_for_each(|val| {
                cursor.append(
                    transition_id,
                    AccountBeforeTx { address: Address::with_last_byte(val), info: None },
                )
            })
            .expect(ERROR_APPEND);
        tx.commit().expect(ERROR_COMMIT);

        // APPEND DUP & APPEND
        let subkey_to_append = 2;
        let tx = db.tx_mut().expect(ERROR_INIT_TX);
        let mut cursor = tx.cursor_write::<AccountChangeSets>().unwrap();
        assert_eq!(
            cursor.append_dup(
                transition_id,
                AccountBeforeTx { address: Address::with_last_byte(subkey_to_append), info: None }
            ),
            Err(DatabaseWriteError {
                info: DatabaseErrorInfo { message: "KeyMismatch".into(), code: 1 },
                operation: DatabaseWriteOperation::CursorAppendDup,
                table_name: AccountChangeSets::NAME,
                key: transition_id.encode().into(),
            }
            .into())
        );
        assert_eq!(
            cursor.append(
                transition_id - 1,
                AccountBeforeTx { address: Address::with_last_byte(subkey_to_append), info: None }
            ),
            Err(DatabaseWriteError {
                info: DatabaseErrorInfo { message: "KeyMismatch".into(), code: 1 },
                operation: DatabaseWriteOperation::CursorAppend,
                table_name: AccountChangeSets::NAME,
                key: (transition_id - 1).encode().into(),
            }
            .into())
        );
        assert_eq!(
            cursor.append(
                transition_id,
                AccountBeforeTx { address: Address::with_last_byte(subkey_to_append), info: None }
            ),
            Ok(())
        );
    }

    #[test]
    fn db_dup_sort() {
        let env = create_test_db(DatabaseEnvKind::RW);
        let key = Address::from_str("0xa2c122be93b0074270ebee7f6b7292c7deb45047")
            .expect(ERROR_ETH_ADDRESS);

        // PUT (0,0)
        let value00 = StorageEntry::default();
        env.update(|tx| tx.put::<PlainStorageState>(key, value00).expect(ERROR_PUT)).unwrap();

        // PUT (2,2)
        let value22 = StorageEntry { key: B256::with_last_byte(2), value: U256::from(2) };
        env.update(|tx| tx.put::<PlainStorageState>(key, value22).expect(ERROR_PUT)).unwrap();

        // PUT (1,1)
        let value11 = StorageEntry { key: B256::with_last_byte(1), value: U256::from(1) };
        env.update(|tx| tx.put::<PlainStorageState>(key, value11).expect(ERROR_PUT)).unwrap();

        // Iterate with cursor
        {
            let tx = env.tx().expect(ERROR_INIT_TX);
            let mut cursor = tx.cursor_dup_read::<PlainStorageState>().unwrap();

            // Notice that value11 and value22 have been ordered in the DB.
            assert!(Some(value00) == cursor.next_dup_val().unwrap());
            assert!(Some(value11) == cursor.next_dup_val().unwrap());
            assert!(Some(value22) == cursor.next_dup_val().unwrap());
        }

        // Seek value with exact subkey
        {
            let tx = env.tx().expect(ERROR_INIT_TX);
            let mut cursor = tx.cursor_dup_read::<PlainStorageState>().unwrap();
            let mut walker = cursor.walk_dup(Some(key), Some(B256::with_last_byte(1))).unwrap();
            assert_eq!(
                (key, value11),
                walker
                    .next()
                    .expect("element should exist.")
                    .expect("should be able to retrieve it.")
            );
        }
    }

    #[test]
    fn db_iterate_over_all_dup_values() {
        let env = create_test_db(DatabaseEnvKind::RW);
        let key1 = Address::from_str("0x1111111111111111111111111111111111111111")
            .expect(ERROR_ETH_ADDRESS);
        let key2 = Address::from_str("0x2222222222222222222222222222222222222222")
            .expect(ERROR_ETH_ADDRESS);

        // PUT key1 (0,0)
        let value00 = StorageEntry::default();
        env.update(|tx| tx.put::<PlainStorageState>(key1, value00).expect(ERROR_PUT)).unwrap();

        // PUT key1 (1,1)
        let value11 = StorageEntry { key: B256::with_last_byte(1), value: U256::from(1) };
        env.update(|tx| tx.put::<PlainStorageState>(key1, value11).expect(ERROR_PUT)).unwrap();

        // PUT key2 (2,2)
        let value22 = StorageEntry { key: B256::with_last_byte(2), value: U256::from(2) };
        env.update(|tx| tx.put::<PlainStorageState>(key2, value22).expect(ERROR_PUT)).unwrap();

        // Iterate with walk_dup
        {
            let tx = env.tx().expect(ERROR_INIT_TX);
            let mut cursor = tx.cursor_dup_read::<PlainStorageState>().unwrap();
            let mut walker = cursor.walk_dup(None, None).unwrap();

            // Notice that value11 and value22 have been ordered in the DB.
            assert_eq!(Some(Ok((key1, value00))), walker.next());
            assert_eq!(Some(Ok((key1, value11))), walker.next());
            // NOTE: Dup cursor does NOT iterates on all values but only on duplicated values of the
            // same key. assert_eq!(Ok(Some(value22.clone())), walker.next());
            assert_eq!(None, walker.next());
        }

        // Iterate by using `walk`
        {
            let tx = env.tx().expect(ERROR_INIT_TX);
            let mut cursor = tx.cursor_dup_read::<PlainStorageState>().unwrap();
            let first = cursor.first().unwrap().unwrap();
            let mut walker = cursor.walk(Some(first.0)).unwrap();
            assert_eq!(Some(Ok((key1, value00))), walker.next());
            assert_eq!(Some(Ok((key1, value11))), walker.next());
            assert_eq!(Some(Ok((key2, value22))), walker.next());
        }
    }

    #[test]
    fn dup_value_with_same_subkey() {
        let env = create_test_db(DatabaseEnvKind::RW);
        let key1 = Address::new([0x11; 20]);
        let key2 = Address::new([0x22; 20]);

        // PUT key1 (0,1)
        let value01 = StorageEntry { key: B256::with_last_byte(0), value: U256::from(1) };
        env.update(|tx| tx.put::<PlainStorageState>(key1, value01).expect(ERROR_PUT)).unwrap();

        // PUT key1 (0,0)
        let value00 = StorageEntry { key: B256::with_last_byte(0), value: U256::from(0) };
        env.update(|tx| tx.put::<PlainStorageState>(key1, value00).expect(ERROR_PUT)).unwrap();

        // PUT key2 (2,2)
        let value22 = StorageEntry { key: B256::with_last_byte(2), value: U256::from(2) };
        env.update(|tx| tx.put::<PlainStorageState>(key2, value22).expect(ERROR_PUT)).unwrap();

        // Iterate with walk
        {
            let tx = env.tx().expect(ERROR_INIT_TX);
            let mut cursor = tx.cursor_dup_read::<PlainStorageState>().unwrap();
            let first = cursor.first().unwrap().unwrap();
            let mut walker = cursor.walk(Some(first.0)).unwrap();

            // NOTE: Both values are present
            assert_eq!(Some(Ok((key1, value00))), walker.next());
            assert_eq!(Some(Ok((key1, value01))), walker.next());
            assert_eq!(Some(Ok((key2, value22))), walker.next());
        }

        // seek_by_key_subkey
        {
            let tx = env.tx().expect(ERROR_INIT_TX);
            let mut cursor = tx.cursor_dup_read::<PlainStorageState>().unwrap();

            // NOTE: There are two values with same SubKey but only first one is shown
            // MM TODO: This is just wrong now isn't it? Why do we care about the order if its not
            // guaranteed anyway?
            assert_eq!(Ok(Some(value00)), cursor.seek_by_key_subkey(key1, value00.key));
            // key1 but value is greater than the one in the DB
            assert_eq!(Ok(None), cursor.seek_by_key_subkey(key1, value22.key));
        }
    }

    #[test]
    fn db_sharded_key() {
        let db: Arc<DatabaseEnv> = create_test_db(DatabaseEnvKind::RW);
        let real_key = Address::from_str("0xa2c122be93b0074270ebee7f6b7292c7deb45047").unwrap();

        for i in 1..5 {
            let key = ShardedKey::new(real_key, i * 100);
            let list: IntegerList = vec![i * 100u64].into();

            db.update(|tx| tx.put::<AccountsHistory>(key.clone(), list.clone()).expect(""))
                .unwrap();
        }

        // Seek value with non existing key.
        {
            let tx = db.tx().expect(ERROR_INIT_TX);
            let mut cursor = tx.cursor_read::<AccountsHistory>().unwrap();

            // It will seek the one greater or equal to the query. Since we have `Address | 100`,
            // `Address | 200` in the database and we're querying `Address | 150` it will return us
            // `Address | 200`.
            let mut walker = cursor.walk(Some(ShardedKey::new(real_key, 150))).unwrap();
            let (key, list) = walker
                .next()
                .expect("element should exist.")
                .expect("should be able to retrieve it.");

            assert_eq!(ShardedKey::new(real_key, 200), key);
            let list200: IntegerList = vec![200u64].into();
            assert_eq!(list200, list);
        }
        // Seek greatest index
        {
            let tx = db.tx().expect(ERROR_INIT_TX);
            let mut cursor = tx.cursor_read::<AccountsHistory>().unwrap();

            // It will seek the MAX value of transition index and try to use prev to get first
            // biggers.
            let _unknown = cursor.seek_exact(ShardedKey::new(real_key, u64::MAX)).unwrap();
            let (key, list) = cursor
                .prev()
                .expect("element should exist.")
                .expect("should be able to retrieve it.");

            assert_eq!(ShardedKey::new(real_key, 400), key);
            let list400: IntegerList = vec![400u64].into();
            assert_eq!(list400, list);
        }
    }
}
