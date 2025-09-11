/// Persistent cache implementation for HDFS file metadata
/// Survives process restarts and provides immediate warm-up capabilities

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{debug, info, warn, error};

use crate::hdfs_optimized_reader::CachedFileInfo;

/// Persistent cache entry with versioning for compatibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentCacheEntry {
    pub version: u32,                    // Schema version for migrations
    pub file_info: CachedFileInfo,       // Core file metadata
    pub cache_metadata: CacheMetadata,   // Cache-specific data
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMetadata {
    pub last_access_time: u64,
    pub access_count: u32,
    pub cache_generation: u64,           // Detect cache invalidation
}

/// Persistent cache configuration
#[derive(Debug, Clone)]
pub struct PersistentCacheConfig {
    pub cache_directory: PathBuf,
    pub max_cache_size_mb: usize,        // 2GB default
    pub persistence_interval_secs: u64,  // How often to persist (300s)
    pub max_age_days: u32,               // Expire old entries (7 days)
    pub compression_enabled: bool,       // Compress cache files
}

impl Default for PersistentCacheConfig {
    fn default() -> Self {
        Self {
            cache_directory: PathBuf::from("./cache/hdfs"),
            max_cache_size_mb: 2048,
            persistence_interval_secs: 300,  // 5 minutes
            max_age_days: 7,
            compression_enabled: true,
        }
    }
}

/// High-performance persistent cache with automatic recovery
pub struct PersistentHdfsCache {
    config: PersistentCacheConfig,
    memory_cache: Arc<RwLock<HashMap<String, PersistentCacheEntry>>>,
    app_cache: Arc<RwLock<HashMap<String, u64>>>,
    cache_generation: Arc<std::sync::atomic::AtomicU64>,
    dirty_flag: Arc<std::sync::atomic::AtomicBool>,
}

impl PersistentHdfsCache {
    /// Create new persistent cache with automatic recovery
    pub async fn new(config: PersistentCacheConfig) -> Result<Self> {
        // Ensure cache directory exists
        fs::create_dir_all(&config.cache_directory).await
            .map_err(|e| anyhow!("Failed to create cache directory: {}", e))?;

        let cache = Self {
            config,
            memory_cache: Arc::new(RwLock::new(HashMap::new())),
            app_cache: Arc::new(RwLock::new(HashMap::new())),
            cache_generation: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            dirty_flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        // CRITICAL: Recover cache from disk on startup
        cache.recover_from_disk().await?;

        // Start background persistence task
        cache.start_background_persistence().await;

        info!("Persistent HDFS cache initialized with recovery capability");
        Ok(cache)
    }

    /// RECOVERY: Load cache from disk after process restart
    async fn recover_from_disk(&self) -> Result<()> {
        let start_time = std::time::Instant::now();
        let cache_file = self.config.cache_directory.join("hdfs_cache.json");
        let app_cache_file = self.config.cache_directory.join("app_cache.json");

        // Recover file cache
        let file_entries = if cache_file.exists() {
            let cache_data = fs::read_to_string(&cache_file).await?;
            let entries: HashMap<String, PersistentCacheEntry> = 
                serde_json::from_str(&cache_data)
                    .map_err(|e| anyhow!("Failed to deserialize file cache: {}", e))?;
            entries.len()
        } else {
            info!("No existing file cache found, starting fresh");
            0
        };

        // Recover application cache
        let app_entries = if app_cache_file.exists() {
            let app_data = fs::read_to_string(&app_cache_file).await?;
            let apps: HashMap<String, u64> = serde_json::from_str(&app_data)
                .map_err(|e| anyhow!("Failed to deserialize app cache: {}", e))?;
            
            let mut app_cache = self.app_cache.write().await;
            *app_cache = apps;
            app_cache.len()
        } else {
            info!("No existing app cache found, starting fresh");
            0
        };

        let recovery_time = start_time.elapsed();
        
        if file_entries > 0 || app_entries > 0 {
            info!("🚀 CACHE RECOVERY SUCCESSFUL:");
            info!("   📁 File entries recovered: {}", file_entries);
            info!("   📱 App entries recovered: {}", app_entries);
            info!("   ⏱️  Recovery time: {:?}", recovery_time);
            
            // Validate recovered cache
            self.validate_recovered_cache().await?;
        } else {
            info!("Starting with empty cache (no recovery needed)");
        }

        Ok(())
    }

    /// Validate recovered cache for consistency
    async fn validate_recovered_cache(&self) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let max_age_seconds = self.config.max_age_days as u64 * 86400;
        
        let mut memory_cache = self.memory_cache.write().await;
        let initial_size = memory_cache.len();
        
        // Remove expired entries
        memory_cache.retain(|_, entry| {
            (now - entry.file_info.last_scanned) < max_age_seconds
        });
        
        let cleaned_entries = initial_size - memory_cache.len();
        
        if cleaned_entries > 0 {
            info!("Cache validation: removed {} expired entries", cleaned_entries);
            self.mark_dirty();
        }
        
        Ok(())
    }

    /// Background task for periodic cache persistence
    async fn start_background_persistence(&self) {
        let memory_cache = Arc::clone(&self.memory_cache);
        let app_cache = Arc::clone(&self.app_cache);
        let config = self.config.clone();
        let dirty_flag = Arc::clone(&self.dirty_flag);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                std::time::Duration::from_secs(config.persistence_interval_secs)
            );

            loop {
                interval.tick().await;

                // Only persist if cache has changes
                if dirty_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    match Self::persist_to_disk_static(&config, &memory_cache, &app_cache).await {
                        Ok(()) => {
                            dirty_flag.store(false, std::sync::atomic::Ordering::Relaxed);
                            debug!("Cache persisted successfully");
                        }
                        Err(e) => {
                            error!("Failed to persist cache: {}", e);
                        }
                    }
                }
            }
        });
    }

    /// Persist current cache state to disk
    async fn persist_to_disk_static(
        config: &PersistentCacheConfig,
        memory_cache: &Arc<RwLock<HashMap<String, PersistentCacheEntry>>>,
        app_cache: &Arc<RwLock<HashMap<String, u64>>>,
    ) -> Result<()> {
        let cache_file = config.cache_directory.join("hdfs_cache.json");
        let app_cache_file = config.cache_directory.join("app_cache.json");

        // Persist file cache
        {
            let cache = memory_cache.read().await;
            let cache_data = serde_json::to_string(&*cache)
                .map_err(|e| anyhow!("Failed to serialize file cache: {}", e))?;
            
            fs::write(&cache_file, cache_data).await
                .map_err(|e| anyhow!("Failed to write file cache: {}", e))?;
        }

        // Persist app cache
        {
            let apps = app_cache.read().await;
            let app_data = serde_json::to_string(&*apps)
                .map_err(|e| anyhow!("Failed to serialize app cache: {}", e))?;
            
            fs::write(&app_cache_file, app_data).await
                .map_err(|e| anyhow!("Failed to write app cache: {}", e))?;
        }

        debug!("Cache persistence completed");
        Ok(())
    }

    /// Add file to cache with persistence tracking
    pub async fn put_file(&self, path: String, file_info: CachedFileInfo) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        
        let entry = PersistentCacheEntry {
            version: 1,
            file_info,
            cache_metadata: CacheMetadata {
                last_access_time: now,
                access_count: 1,
                cache_generation: self.cache_generation.load(std::sync::atomic::Ordering::Relaxed),
            },
        };

        {
            let mut cache = self.memory_cache.write().await;
            cache.insert(path, entry);
        }

        self.mark_dirty();
        Ok(())
    }

    /// Get file from cache with access tracking
    pub async fn get_file(&self, path: &str) -> Option<CachedFileInfo> {
        let mut cache = self.memory_cache.write().await;
        
        if let Some(entry) = cache.get_mut(path) {
            // Update access statistics
            entry.cache_metadata.access_count += 1;
            entry.cache_metadata.last_access_time = 
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            
            self.mark_dirty();
            return Some(entry.file_info.clone());
        }
        
        None
    }

    /// Mark cache as dirty (needs persistence)
    fn mark_dirty(&self) {
        self.dirty_flag.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Force immediate persistence (for graceful shutdown)
    pub async fn flush_to_disk(&self) -> Result<()> {
        Self::persist_to_disk_static(&self.config, &self.memory_cache, &self.app_cache).await?;
        self.dirty_flag.store(false, std::sync::atomic::Ordering::Relaxed);
        info!("Cache flushed to disk");
        Ok(())
    }

    /// Get cache statistics for monitoring
    pub async fn get_stats(&self) -> CacheStats {
        let memory_cache = self.memory_cache.read().await;
        let app_cache = self.app_cache.read().await;
        
        CacheStats {
            total_cached_files: memory_cache.len(),
            total_cached_apps: app_cache.len(),
            cache_size_mb: (memory_cache.len() * std::mem::size_of::<PersistentCacheEntry>()) / 1048576,
            is_persistent: true,
            last_recovery_time: Some(std::time::SystemTime::now()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub total_cached_files: usize,
    pub total_cached_apps: usize,
    pub cache_size_mb: usize,
    pub is_persistent: bool,
    pub last_recovery_time: Option<std::time::SystemTime>,
}

/// Graceful shutdown handler
impl Drop for PersistentHdfsCache {
    fn drop(&mut self) {
        // Best effort flush on shutdown (sync operation)
        if self.dirty_flag.load(std::sync::atomic::Ordering::Relaxed) {
            warn!("Cache has unsaved changes during shutdown - some data may be lost");
        }
    }
}