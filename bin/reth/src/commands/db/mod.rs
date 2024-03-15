//! Database debugging tool

use crate::{
    args::{
        utils::{chain_help, genesis_value_parser, SUPPORTED_CHAINS},
        DatabaseArgs,
    },
    dirs::{DataDirPath, MaybePlatformPath},
    utils::DbTool,
};
use clap::{Parser, Subcommand};
use reth_db::{
    open_db, open_db_read_only,
    version::{get_db_version, DatabaseVersionError, DB_VERSION},
};
use reth_primitives::ChainSpec;
use reth_provider::ProviderFactory;
use std::{
    io::{self, Write},
    sync::Arc,
};

#[cfg(feature = "mdbx")]
mod clear;
#[cfg(feature = "mdbx")]
mod diff;
mod get;
#[cfg(feature = "mdbx")]
mod list;
#[cfg(feature = "mdbx")]
mod static_files;
#[cfg(feature = "mdbx")]
mod stats;
/// DB List TUI
#[cfg(feature = "mdbx")]
mod tui;

/// `reth db` command
#[derive(Debug, Parser)]
pub struct Command {
    /// The path to the data dir for all reth files and subdirectories.
    ///
    /// Defaults to the OS-specific data directory:
    ///
    /// - Linux: `$XDG_DATA_HOME/reth/` or `$HOME/.local/share/reth/`
    /// - Windows: `{FOLDERID_RoamingAppData}/reth/`
    /// - macOS: `$HOME/Library/Application Support/reth/`
    #[arg(long, value_name = "DATA_DIR", verbatim_doc_comment, default_value_t, global = true)]
    datadir: MaybePlatformPath<DataDirPath>,

    /// The chain this node is running.
    ///
    /// Possible values are either a built-in chain or the path to a chain specification file.
    #[arg(
        long,
        value_name = "CHAIN_OR_PATH",
        long_help = chain_help(),
        default_value = SUPPORTED_CHAINS[0],
        value_parser = genesis_value_parser,
        global = true,
    )]
    chain: Arc<ChainSpec>,

    #[command(flatten)]
    db: DatabaseArgs,

    #[command(subcommand)]
    command: Subcommands,
}

#[derive(Subcommand, Debug)]
/// `reth db` subcommands
pub enum Subcommands {
    /// Lists all the tables, their entry count and their size
#[cfg(feature = "mdbx")]
    Stats(stats::Command),
    /// Lists the contents of a table
#[cfg(feature = "mdbx")]
    List(list::Command),
    /// Create a diff between two database tables or two entire databases.
#[cfg(feature = "mdbx")]
    Diff(diff::Command),
    /// Gets the content of a table for the given key
#[cfg(feature = "mdbx")]
    Get(get::Command),
    /// Deletes all database entries
#[cfg(feature = "mdbx")]
    Drop {
        /// Bypasses the interactive confirmation and drops the database directly
        #[arg(short, long)]
        force: bool,
    },
    /// Deletes all table entries
#[cfg(feature = "mdbx")]
    Clear(clear::Command),
    /// Creates static files from database tables
#[cfg(feature = "mdbx")]
    CreateStaticFiles(static_files::Command),
    /// Lists current and local database versions
    Version,
    /// Returns the full database path
    Path,
}

/// db_ro_exec opens a database in read-only mode, and then execute with the provided command
macro_rules! db_ro_exec {
    ($chain:expr, $db_path:expr, $db_args:ident, $sfp:ident, $tool:ident, $command:block) => {
        let db = open_db_read_only($db_path, $db_args)?;
        let provider_factory = ProviderFactory::new(db, $chain.clone(), $sfp)?;

        let $tool = DbTool::new(provider_factory, $chain.clone())?;
        $command;
    };
}

impl Command {
    /// Execute `db` command
    pub async fn execute(self) -> eyre::Result<()> {
        // add network name to data dir
        let data_dir = self.datadir.unwrap_or_chain_default(self.chain.chain);
        let db_path = data_dir.db_path();
        let db_args = self.db.database_args();
        let static_files_path = data_dir.static_files_path();

        match self.command {
            // TODO: We'll need to add this on the DB trait.
#[cfg(feature = "mdbx")]
            Subcommands::Stats(command) => {
                db_ro_exec!(self.chain, &db_path, db_args, static_files_path, tool, {
                    command.execute(data_dir, &tool)?;
                });
            }
#[cfg(feature = "mdbx")]
            Subcommands::List(command) => {
                db_ro_exec!(self.chain, &db_path, db_args, static_files_path, tool, {
                    command.execute(&tool)?;
                });
            }
#[cfg(feature = "mdbx")]
            Subcommands::Diff(command) => {
                db_ro_exec!(self.chain, &db_path, db_args, static_files_path, tool, {
                    command.execute(&tool)?;
                });
            }
#[cfg(feature = "mdbx")]
            Subcommands::Get(command) => {
                db_ro_exec!(self.chain, &db_path, db_args, static_files_path, tool, {
                    command.execute(&tool)?;
                });
            }
#[cfg(feature = "mdbx")]
            Subcommands::Drop { force } => {
                if !force {
                    // Ask for confirmation
                    print!("Are you sure you want to drop the database at {data_dir}? This cannot be undone. (y/N): ");
                    // Flush the buffer to ensure the message is printed immediately
                    io::stdout().flush().unwrap();

                    let mut input = String::new();
                    io::stdin().read_line(&mut input).expect("Failed to read line");

                    if !input.trim().eq_ignore_ascii_case("y") {
                        println!("Database drop aborted!");
                        return Ok(())
                    }
                }

                let db = open_db(&db_path, db_args)?;
                let provider_factory =
                    ProviderFactory::new(db, self.chain.clone(), static_files_path.clone())?;

                let mut tool = DbTool::new(provider_factory, self.chain.clone())?;
                tool.drop(db_path, static_files_path)?;
            }
#[cfg(feature = "mdbx")]
            Subcommands::Clear(command) => {
                let db = open_db(&db_path, db_args)?;
                let provider_factory =
                    ProviderFactory::new(db, self.chain.clone(), static_files_path)?;

                command.execute(provider_factory)?;
            }
#[cfg(feature = "mdbx")]
            Subcommands::CreateStaticFiles(command) => {
                command.execute(data_dir, self.db.database_args(), self.chain.clone())?;
            }
            Subcommands::Version => {
                let local_db_version = match get_db_version(&db_path) {
                    Ok(version) => Some(version),
                    Err(DatabaseVersionError::MissingFile) => None,
                    Err(err) => return Err(err.into()),
                };

                println!("Current database version: {DB_VERSION}");

                if let Some(version) = local_db_version {
                    println!("Local database version: {version}");
                } else {
                    println!("Local database is uninitialized");
                }
            }
            Subcommands::Path => {
                println!("{}", db_path.display());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn parse_stats_globals() {
        let path = format!("../{}", SUPPORTED_CHAINS[0]);
        let cmd = Command::try_parse_from(["reth", "stats", "--datadir", &path]).unwrap();
        assert_eq!(cmd.datadir.as_ref(), Some(Path::new(&path)));
    }
}
