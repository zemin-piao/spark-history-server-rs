/// Storage backend abstraction for analytical data processing
/// Supports multiple backends (DuckDB, ClickHouse, PostgreSQL, etc.)

use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;

pub mod duckdb_store;
pub mod backend_trait;
pub mod file_reader;
pub mod event_log;

// Re-export commonly used types
pub use duckdb_store::{DuckDbStore, SparkEvent};
pub use backend_trait::{AnalyticalStorageBackend, StorageBackendType, BackendConfig};

// Compatibility type alias for existing code
pub type HistoryProvider = Box<dyn AnalyticalStorageBackend + Send + Sync>;

/// Configuration for different storage backends
#[derive(Debug, Clone)]
pub enum StorageConfig {
    DuckDB {
        database_path: String,
        num_workers: usize,
        batch_size: usize,
    },
    ClickHouse {
        connection_url: String,
        database: String,
        table: String,
        batch_size: usize,
    },
    PostgreSQL {
        connection_url: String,
        database: String,
        schema: String,
        table: String,
        pool_size: usize,
    },
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig::DuckDB {
            database_path: "./data/events.db".to_string(),
            num_workers: 8,
            batch_size: 5000,
        }
    }
}

/// Factory for creating storage backends
pub struct StorageBackendFactory;

impl StorageBackendFactory {
    /// Create a storage backend from configuration
    pub async fn create_backend(config: StorageConfig) -> Result<Box<dyn AnalyticalStorageBackend + Send + Sync>> {
        match config {
            StorageConfig::DuckDB { database_path, num_workers, batch_size } => {
                let store = DuckDbStore::new_with_config(&database_path, num_workers, batch_size).await?;
                Ok(Box::new(store))
            }
            StorageConfig::ClickHouse { .. } => {
                // Placeholder for future ClickHouse implementation
                unimplemented!("ClickHouse backend not yet implemented")
            }
            StorageConfig::PostgreSQL { .. } => {
                // Placeholder for future PostgreSQL implementation  
                unimplemented!("PostgreSQL backend not yet implemented")
            }
        }
    }
}