/// High-performance HDFS reader optimized for millions of files
/// 
/// Key optimizations:
/// 1. Bulk directory listings with caching
/// 2. Hierarchical file discovery
/// 3. Incremental change detection
/// 4. Batch metadata operations

use anyhow::{anyhow, Result};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, info, warn, error};

use crate::hdfs_reader::{HdfsReader, HdfsFileInfo};

/// File cache entry with metadata and timestamps
#[derive(Debug, Clone)]
pub struct CachedFileInfo {
    pub path: String,
    pub size: u64,
    pub modification_time: u64,
    pub is_complete: bool,
    pub last_scanned: u64,
}

/// Optimized HDFS scanner for massive file sets
pub struct OptimizedHdfsReader {
    inner: Arc<HdfsReader>,
    file_cache: Arc<RwLock<HashMap<String, CachedFileInfo>>>,
    app_cache: Arc<RwLock<HashMap<String, u64>>>, // app_id -> last_scan_time
    base_path: String,
    cache_ttl_seconds: u64,
}

impl OptimizedHdfsReader {
    pub async fn new(hdfs_reader: HdfsReader, base_path: String) -> Self {
        Self {
            inner: Arc::new(hdfs_reader),
            file_cache: Arc::new(RwLock::new(HashMap::new())),
            app_cache: Arc::new(RwLock::new(HashMap::new())),
            base_path,
            cache_ttl_seconds: 300, // 5 minutes cache TTL
        }
    }

    /// High-performance bulk application discovery
    /// Instead of 40K individual calls, makes 1 bulk call + smart caching
    pub async fn discover_applications_bulk(&self) -> Result<Vec<String>> {
        let start_time = std::time::Instant::now();
        
        // Check if we need to refresh the application cache
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let app_cache = self.app_cache.read().await;
        
        let needs_refresh = app_cache.is_empty() || 
            app_cache.values().any(|&last_scan| now - last_scan > self.cache_ttl_seconds);
        
        if !needs_refresh {
            let cached_apps: Vec<String> = app_cache.keys().cloned().collect();
            info!("Using cached applications: {} apps", cached_apps.len());
            return Ok(cached_apps);
        }
        
        drop(app_cache);

        info!("🔄 Performing bulk application discovery on HDFS: {}", self.base_path);

        // OPTIMIZATION 1: Single bulk directory listing
        let applications = self.inner.list_applications(&self.base_path).await?;
        
        // OPTIMIZATION 2: Update application cache
        let mut app_cache = self.app_cache.write().await;
        app_cache.clear();
        for app in &applications {
            app_cache.insert(app.clone(), now);
        }
        drop(app_cache);

        let duration = start_time.elapsed();
        info!("✅ Bulk application discovery completed: {} apps in {:?}", 
              applications.len(), duration);

        Ok(applications)
    }

    /// Massively optimized file discovery using hierarchical scanning
    /// Key optimizations:
    /// 1. Parallel directory scanning
    /// 2. Incremental change detection
    /// 3. Smart caching with TTL
    pub async fn discover_changed_files_bulk(&self) -> Result<HashMap<String, Vec<CachedFileInfo>>> {
        let start_time = std::time::Instant::now();
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        info!("🚀 Starting optimized bulk file discovery...");

        // Get applications (from cache if possible)
        let applications = self.discover_applications_bulk().await?;
        
        // OPTIMIZATION 1: Parallel directory processing with semaphore
        let semaphore = Arc::new(tokio::sync::Semaphore::new(20)); // Limit concurrent HDFS operations
        let mut handles = Vec::new();

        for app_id in applications {
            let permit = semaphore.clone().acquire_owned().await?;
            let inner = Arc::clone(&self.inner);
            let file_cache = Arc::clone(&self.file_cache);
            let base_path = self.base_path.clone();
            let cache_ttl = self.cache_ttl_seconds;

            let handle = tokio::spawn(async move {
                let _permit = permit; // Hold semaphore permit
                Self::scan_application_files(inner, file_cache, base_path, app_id, now, cache_ttl).await
            });

            handles.push(handle);
        }

        // Collect results from parallel scanning
        let mut all_changed_files = HashMap::new();
        let mut total_files = 0;
        let mut changed_files = 0;

        for handle in handles {
            match handle.await? {
                Ok((app_id, files)) => {
                    if !files.is_empty() {
                        total_files += files.len();
                        changed_files += files.iter().filter(|f| f.last_scanned == now).count();
                        all_changed_files.insert(app_id, files);
                    }
                }
                Err(e) => {
                    warn!("Application scanning failed: {}", e);
                }
            }
        }

        let duration = start_time.elapsed();
        let throughput = total_files as f64 / duration.as_secs_f64();

        info!("✅ Bulk file discovery completed:");
        info!("   📊 Total files scanned: {}", total_files);
        info!("   🔄 Changed files detected: {}", changed_files);
        info!("   ⏱️  Scan duration: {:?}", duration);
        info!("   🚀 Throughput: {:.0} files/sec", throughput);

        Ok(all_changed_files)
    }

    /// Scan a single application's files with smart caching
    async fn scan_application_files(
        hdfs_reader: Arc<HdfsReader>,
        file_cache: Arc<RwLock<HashMap<String, CachedFileInfo>>>,
        base_path: String,
        app_id: String,
        now: u64,
        cache_ttl: u64,
    ) -> Result<(String, Vec<CachedFileInfo>)> {
        
        // OPTIMIZATION 1: Check cache first
        {
            let cache = file_cache.read().await;
            let app_files: Vec<_> = cache
                .values()
                .filter(|f| f.path.contains(&app_id) && (now - f.last_scanned) < cache_ttl)
                .cloned()
                .collect();
            
            if !app_files.is_empty() {
                debug!("Using cached files for app {}: {} files", app_id, app_files.len());
                return Ok((app_id, app_files));
            }
        }

        // OPTIMIZATION 2: Bulk file listing for application
        let app_files = match hdfs_reader.list_event_files(&base_path, &app_id).await {
            Ok(files) => files,
            Err(e) => {
                warn!("Failed to list files for app {}: {}", app_id, e);
                return Ok((app_id, vec![]));
            }
        };

        // OPTIMIZATION 3: Batch metadata retrieval
        let mut cached_files = Vec::new();
        let mut cache_updates = Vec::new();

        for file_path in app_files {
            // Get file info (this could be further optimized with bulk stat operations)
            match hdfs_reader.get_file_info(&file_path).await {
                Ok(file_info) => {
                    let cached_file = CachedFileInfo {
                        path: file_path.clone(),
                        size: file_info.size,
                        modification_time: file_info.modification_time,
                        is_complete: !file_path.ends_with(".inprogress"),
                        last_scanned: now,
                    };

                    cached_files.push(cached_file.clone());
                    cache_updates.push((file_path, cached_file));
                }
                Err(e) => {
                    debug!("Failed to get file info for {}: {}", file_path, e);
                }
            }
        }

        // OPTIMIZATION 4: Batch cache updates
        {
            let mut cache = file_cache.write().await;
            for (path, file_info) in cache_updates {
                cache.insert(path, file_info);
            }
        }

        Ok((app_id, cached_files))
    }

    /// Smart incremental change detection
    /// Only processes files that have actually changed
    pub async fn get_changed_files_since(&self, since_timestamp: u64) -> Result<Vec<CachedFileInfo>> {
        let file_cache = self.file_cache.read().await;
        
        let changed_files: Vec<CachedFileInfo> = file_cache
            .values()
            .filter(|file| file.modification_time > since_timestamp)
            .cloned()
            .collect();

        info!("Found {} changed files since timestamp {}", changed_files.len(), since_timestamp);
        Ok(changed_files)
    }

    /// Get cache statistics for monitoring
    pub async fn get_cache_stats(&self) -> CacheStats {
        let file_cache = self.file_cache.read().await;
        let app_cache = self.app_cache.read().await;

        CacheStats {
            total_cached_files: file_cache.len(),
            total_cached_apps: app_cache.len(),
            cache_size_mb: (file_cache.len() * std::mem::size_of::<CachedFileInfo>()) / 1048576,
        }
    }

    /// Clear expired cache entries
    pub async fn cleanup_cache(&self) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        
        // Clean file cache
        let mut file_cache = self.file_cache.write().await;
        let initial_size = file_cache.len();
        file_cache.retain(|_, file| (now - file.last_scanned) < self.cache_ttl_seconds * 2);
        let cleaned_files = initial_size - file_cache.len();

        // Clean app cache  
        let mut app_cache = self.app_cache.write().await;
        let initial_apps = app_cache.len();
        app_cache.retain(|_, &mut last_scan| (now - last_scan) < self.cache_ttl_seconds * 2);
        let cleaned_apps = initial_apps - app_cache.len();

        info!("Cache cleanup: removed {} files, {} apps", cleaned_files, cleaned_apps);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub total_cached_files: usize,
    pub total_cached_apps: usize,
    pub cache_size_mb: usize,
}

/// Production-optimized scanning strategy
/// 
/// Performance Targets for 40K applications:
/// - Cold scan: <2 minutes (vs 30+ minutes with naive approach)
/// - Warm scan: <30 seconds 
/// - Memory usage: <2GB for cache
/// - HDFS NameNode RPS: <1000 (vs 80K+ with naive approach)
impl OptimizedHdfsReader {
    /// Entry point for production scanning
    pub async fn production_scan(&self) -> Result<HashMap<String, Vec<CachedFileInfo>>> {
        // Start cache cleanup in background
        let cleanup_cache = Arc::clone(&self.file_cache);
        let cleanup_ttl = self.cache_ttl_seconds;
        tokio::spawn(async move {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            let mut cache = cleanup_cache.write().await;
            cache.retain(|_, file| (now - file.last_scanned) < cleanup_ttl);
        });

        // Perform optimized bulk discovery
        self.discover_changed_files_bulk().await
    }
}