use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::path::Path;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

use crate::hdfs_reader::FileMetadata;

/// Simple file-based metadata store for tracking file changes
pub struct MetadataStore {
    metadata_file: std::path::PathBuf,
    metadata: RwLock<HashMap<String, FileMetadata>>,
}

impl MetadataStore {
    /// Create a new metadata store
    pub async fn new(metadata_path: &Path) -> Result<Self> {
        let metadata_file = metadata_path.join("file_metadata.json");
        
        let store = Self {
            metadata_file: metadata_file.clone(),
            metadata: RwLock::new(HashMap::new()),
        };

        // Load existing metadata if file exists
        if metadata_file.exists() {
            if let Err(e) = store.load_metadata().await {
                error!("Failed to load existing metadata: {}", e);
            }
        }

        Ok(store)
    }

    /// Load metadata from disk
    async fn load_metadata(&self) -> Result<()> {
        let content = tokio::fs::read_to_string(&self.metadata_file)
            .await
            .map_err(|e| anyhow!("Failed to read metadata file: {}", e))?;

        let metadata: HashMap<String, FileMetadata> = serde_json::from_str(&content)
            .map_err(|e| anyhow!("Failed to parse metadata JSON: {}", e))?;

        let mut store_metadata = self.metadata.write().await;
        *store_metadata = metadata;
        
        info!("Loaded {} file metadata records", store_metadata.len());
        Ok(())
    }

    /// Save metadata to disk
    async fn save_metadata(&self) -> Result<()> {
        let metadata = self.metadata.read().await;
        let json = serde_json::to_string_pretty(&*metadata)
            .map_err(|e| anyhow!("Failed to serialize metadata: {}", e))?;

        // Ensure parent directory exists
        if let Some(parent) = self.metadata_file.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| anyhow!("Failed to create metadata directory: {}", e))?;
        }

        tokio::fs::write(&self.metadata_file, json)
            .await
            .map_err(|e| anyhow!("Failed to write metadata file: {}", e))?;

        debug!("Saved {} metadata records to disk", metadata.len());
        Ok(())
    }

    /// Get metadata for a specific file
    pub async fn get_metadata(&self, file_path: &str) -> Option<FileMetadata> {
        let metadata = self.metadata.read().await;
        metadata.get(file_path).cloned()
    }

    /// Update metadata for a file
    pub async fn update_metadata(&self, file_metadata: FileMetadata) -> Result<()> {
        {
            let mut metadata = self.metadata.write().await;
            metadata.insert(file_metadata.path.clone(), file_metadata);
        }

        // Save to disk
        self.save_metadata().await?;
        Ok(())
    }

    /// Check if a file should be reloaded based on size comparison
    pub async fn should_reload_file(&self, file_path: &str, current_size: i64) -> bool {
        match self.get_metadata(file_path).await {
            Some(metadata) => {
                // Reload if file size has increased (new events appended)
                current_size > metadata.file_size
            }
            None => {
                // New file, should be loaded
                true
            }
        }
    }

    /// Get all tracked files
    pub async fn get_all_tracked_files(&self) -> Vec<String> {
        let metadata = self.metadata.read().await;
        metadata.keys().cloned().collect()
    }

    /// Remove metadata for a file (e.g., if file was deleted)
    pub async fn remove_metadata(&self, file_path: &str) -> Result<()> {
        {
            let mut metadata = self.metadata.write().await;
            metadata.remove(file_path);
        }

        self.save_metadata().await?;
        Ok(())
    }

    /// Get statistics about tracked files
    pub async fn get_stats(&self) -> MetadataStats {
        let metadata = self.metadata.read().await;
        let total_files = metadata.len();
        let complete_files = metadata.values().filter(|m| m.is_complete).count();
        let incomplete_files = total_files - complete_files;

        MetadataStats {
            total_files,
            complete_files,
            incomplete_files,
        }
    }
}

/// Statistics about metadata store
#[derive(Debug, Clone)]
pub struct MetadataStats {
    pub total_files: usize,
    pub complete_files: usize,
    pub incomplete_files: usize,
}