use anyhow::{anyhow, Result};
use rocksdb::{DB, IteratorMode, Options};
use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
};
use tokio::sync::RwLock;
use tracing::info;

use crate::models::ApplicationInfo;

/// Key-value store trait for application storage
#[async_trait::async_trait]
pub trait ApplicationStore: Send + Sync {
    async fn get(&self, app_id: &str) -> Result<Option<ApplicationInfo>>;
    async fn put(&self, app_id: &str, app: ApplicationInfo) -> Result<()>;
    async fn list(&self) -> Result<Vec<ApplicationInfo>>;
    async fn remove(&self, app_id: &str) -> Result<()>;
    async fn contains(&self, app_id: &str) -> Result<bool>;
    async fn count(&self) -> Result<usize>;
}

/// In-memory store for fast access during loading
#[derive(Debug)]
pub struct InMemoryStore {
    data: Arc<RwLock<HashMap<String, ApplicationInfo>>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl ApplicationStore for InMemoryStore {
    async fn get(&self, app_id: &str) -> Result<Option<ApplicationInfo>> {
        let data = self.data.read().await;
        Ok(data.get(app_id).cloned())
    }

    async fn put(&self, app_id: &str, app: ApplicationInfo) -> Result<()> {
        let mut data = self.data.write().await;
        data.insert(app_id.to_string(), app);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<ApplicationInfo>> {
        let data = self.data.read().await;
        Ok(data.values().cloned().collect())
    }

    async fn remove(&self, app_id: &str) -> Result<()> {
        let mut data = self.data.write().await;
        data.remove(app_id);
        Ok(())
    }

    async fn contains(&self, app_id: &str) -> Result<bool> {
        let data = self.data.read().await;
        Ok(data.contains_key(app_id))
    }

    async fn count(&self) -> Result<usize> {
        let data = self.data.read().await;
        Ok(data.len())
    }
}

/// Persistent RocksDB store for disk-based caching
#[derive(Debug)]
pub struct RocksDbStore {
    db: Arc<DB>,
}

impl RocksDbStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        
        let db = DB::open(&opts, path)?;
        Ok(Self {
            db: Arc::new(db),
        })
    }

    fn serialize_app(app: &ApplicationInfo) -> Result<Vec<u8>> {
        let json = serde_json::to_string(app)?;
        Ok(json.into_bytes())
    }

    fn deserialize_app(data: &[u8]) -> Result<ApplicationInfo> {
        let json = String::from_utf8(data.to_vec())?;
        let app: ApplicationInfo = serde_json::from_str(&json)?;
        Ok(app)
    }
}

#[async_trait::async_trait]
impl ApplicationStore for RocksDbStore {
    async fn get(&self, app_id: &str) -> Result<Option<ApplicationInfo>> {
        match self.db.get(app_id.as_bytes())? {
            Some(data) => Ok(Some(Self::deserialize_app(&data)?)),
            None => Ok(None),
        }
    }

    async fn put(&self, app_id: &str, app: ApplicationInfo) -> Result<()> {
        let data = Self::serialize_app(&app)?;
        self.db.put(app_id.as_bytes(), &data)?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<ApplicationInfo>> {
        let mut apps = Vec::new();
        let iter = self.db.iterator(IteratorMode::Start);
        
        for item in iter {
            let (_key, value) = item?;
            let app = Self::deserialize_app(&value)?;
            apps.push(app);
        }
        
        Ok(apps)
    }

    async fn remove(&self, app_id: &str) -> Result<()> {
        self.db.delete(app_id.as_bytes())?;
        Ok(())
    }

    async fn contains(&self, app_id: &str) -> Result<bool> {
        Ok(self.db.get(app_id.as_bytes())?.is_some())
    }

    async fn count(&self) -> Result<usize> {
        let mut count = 0;
        let iter = self.db.iterator(IteratorMode::Start);
        for item in iter {
            let _ = item?;
            count += 1;
        }
        Ok(count)
    }
}

/// Storage states for the hybrid store
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageState {
    InMemory,
    Transitioning,
    Persistent,
}

/// Hybrid store that combines in-memory and persistent storage
/// Similar to Spark's HybridStore implementation
#[derive(Debug)]
pub struct HybridStore {
    memory_store: InMemoryStore,
    disk_store: Option<RocksDbStore>,
    state: Arc<RwLock<StorageState>>,
}

impl HybridStore {
    pub fn new() -> Self {
        Self {
            memory_store: InMemoryStore::new(),
            disk_store: None,
            state: Arc::new(RwLock::new(StorageState::InMemory)),
        }
    }

    pub fn with_disk_store<P: AsRef<Path>>(mut self, path: P) -> Result<Self> {
        let disk_store = RocksDbStore::new(path)?;
        self.disk_store = Some(disk_store);
        Ok(self)
    }

    /// Initialize persistent storage and start background migration
    pub async fn initialize_disk_store<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        if self.disk_store.is_some() {
            return Ok(());
        }

        info!("Initializing RocksDB store at: {:?}", path.as_ref());
        self.disk_store = Some(RocksDbStore::new(path)?);
        Ok(())
    }

    /// Switch to persistent storage after initial loading is complete
    pub async fn switch_to_persistent(&self) -> Result<()> {
        let mut state = self.state.write().await;
        if *state != StorageState::InMemory {
            return Ok(());
        }

        let disk_store = self.disk_store.as_ref()
            .ok_or_else(|| anyhow!("Disk store not initialized"))?;

        info!("Switching to persistent storage...");
        *state = StorageState::Transitioning;

        // Migrate all data from memory to disk
        let apps = self.memory_store.list().await?;
        info!("Found {} applications to migrate", apps.len());
        for app in apps {
            let app_id = app.id.clone();
            info!("Migrating application: {}", app_id);
            disk_store.put(&app_id, app).await?;
        }
        info!("Migration complete");

        *state = StorageState::Persistent;
        info!("Successfully switched to persistent storage");
        Ok(())
    }

    /// Get the current storage state
    pub async fn get_state(&self) -> StorageState {
        *self.state.read().await
    }

    /// Get the appropriate store based on current state
    async fn get_active_store(&self) -> &dyn ApplicationStore {
        let state = *self.state.read().await;
        match state {
            StorageState::InMemory | StorageState::Transitioning => {
                &self.memory_store
            }
            StorageState::Persistent => {
                if let Some(ref disk_store) = self.disk_store {
                    disk_store
                } else {
                    &self.memory_store
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl ApplicationStore for HybridStore {
    async fn get(&self, app_id: &str) -> Result<Option<ApplicationInfo>> {
        let state = *self.state.read().await;
        match state {
            StorageState::InMemory | StorageState::Transitioning => {
                self.memory_store.get(app_id).await
            }
            StorageState::Persistent => {
                if let Some(ref disk_store) = self.disk_store {
                    disk_store.get(app_id).await
                } else {
                    self.memory_store.get(app_id).await
                }
            }
        }
    }

    async fn put(&self, app_id: &str, app: ApplicationInfo) -> Result<()> {
        let state = *self.state.read().await;
        match state {
            StorageState::InMemory => {
                self.memory_store.put(app_id, app).await
            }
            StorageState::Transitioning => {
                // During transition, write to both stores
                self.memory_store.put(app_id, app.clone()).await?;
                if let Some(ref disk_store) = self.disk_store {
                    disk_store.put(app_id, app).await?;
                }
                Ok(())
            }
            StorageState::Persistent => {
                if let Some(ref disk_store) = self.disk_store {
                    disk_store.put(app_id, app).await
                } else {
                    self.memory_store.put(app_id, app).await
                }
            }
        }
    }

    async fn list(&self) -> Result<Vec<ApplicationInfo>> {
        let state = *self.state.read().await;
        match state {
            StorageState::InMemory | StorageState::Transitioning => {
                self.memory_store.list().await
            }
            StorageState::Persistent => {
                if let Some(ref disk_store) = self.disk_store {
                    disk_store.list().await
                } else {
                    self.memory_store.list().await
                }
            }
        }
    }

    async fn remove(&self, app_id: &str) -> Result<()> {
        let state = *self.state.read().await;
        match state {
            StorageState::InMemory => {
                self.memory_store.remove(app_id).await
            }
            StorageState::Transitioning => {
                // During transition, remove from both stores
                self.memory_store.remove(app_id).await?;
                if let Some(ref disk_store) = self.disk_store {
                    disk_store.remove(app_id).await?;
                }
                Ok(())
            }
            StorageState::Persistent => {
                if let Some(ref disk_store) = self.disk_store {
                    disk_store.remove(app_id).await
                } else {
                    self.memory_store.remove(app_id).await
                }
            }
        }
    }

    async fn contains(&self, app_id: &str) -> Result<bool> {
        let state = *self.state.read().await;
        match state {
            StorageState::InMemory | StorageState::Transitioning => {
                self.memory_store.contains(app_id).await
            }
            StorageState::Persistent => {
                if let Some(ref disk_store) = self.disk_store {
                    disk_store.contains(app_id).await
                } else {
                    self.memory_store.contains(app_id).await
                }
            }
        }
    }

    async fn count(&self) -> Result<usize> {
        let state = *self.state.read().await;
        match state {
            StorageState::InMemory | StorageState::Transitioning => {
                self.memory_store.count().await
            }
            StorageState::Persistent => {
                if let Some(ref disk_store) = self.disk_store {
                    disk_store.count().await
                } else {
                    self.memory_store.count().await
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ApplicationAttemptInfo, ApplicationInfo};
    use chrono::Utc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_in_memory_store() {
        let store = InMemoryStore::new();
        let app = create_test_app();

        assert!(!store.contains("test-app").await.unwrap());
        
        store.put("test-app", app.clone()).await.unwrap();
        assert!(store.contains("test-app").await.unwrap());
        
        let retrieved = store.get("test-app").await.unwrap().unwrap();
        assert_eq!(retrieved.id, app.id);
        
        assert_eq!(store.count().await.unwrap(), 1);
        
        store.remove("test-app").await.unwrap();
        assert!(!store.contains("test-app").await.unwrap());
    }

    #[tokio::test]
    async fn test_rocksdb_store() {
        let temp_dir = tempdir().unwrap();
        let store = RocksDbStore::new(temp_dir.path()).unwrap();
        let app = create_test_app();

        assert!(!store.contains("test-app").await.unwrap());
        
        store.put("test-app", app.clone()).await.unwrap();
        assert!(store.contains("test-app").await.unwrap());
        
        let retrieved = store.get("test-app").await.unwrap().unwrap();
        assert_eq!(retrieved.id, app.id);
        
        assert_eq!(store.count().await.unwrap(), 1);
        
        store.remove("test-app").await.unwrap();
        assert!(!store.contains("test-app").await.unwrap());
    }

    #[tokio::test]
    async fn test_hybrid_store_transition() {
        let temp_dir = tempdir().unwrap();
        let mut store = HybridStore::new();
        store.initialize_disk_store(temp_dir.path()).await.unwrap();
        
        let app = create_test_app();
        
        // Initially in memory
        assert_eq!(store.get_state().await, StorageState::InMemory);
        
        let app_key = app.id.clone(); // Use the app's actual ID as the key
        store.put(&app_key, app.clone()).await.unwrap();
        assert!(store.contains(&app_key).await.unwrap());
        
        // Verify data is in memory before transition
        let retrieved_memory = store.get(&app_key).await.unwrap().unwrap();
        assert_eq!(retrieved_memory.id, app.id);
        
        
        // Switch to persistent
        store.switch_to_persistent().await.unwrap();
        assert_eq!(store.get_state().await, StorageState::Persistent);
        
        // Should still be able to access the data from persistent storage
        let retrieved_persistent = store.get(&app_key).await.unwrap().unwrap();
        assert_eq!(retrieved_persistent.id, app.id);
    }

    fn create_test_app() -> ApplicationInfo {
        let attempt = ApplicationAttemptInfo::new(
            Some("1".to_string()),
            Utc::now(),
            Utc::now(),
            Utc::now(),
            "test-user".to_string(),
            true,
            "3.5.0".to_string(),
        );

        ApplicationInfo {
            id: "test-app-123".to_string(),
            name: "Test Application".to_string(),
            cores_granted: Some(4),
            max_cores: Some(8),
            cores_per_executor: Some(2),
            memory_per_executor_mb: Some(1024),
            attempts: vec![attempt],
        }
    }
}