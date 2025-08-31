use anyhow::Result;
use async_trait::async_trait;
use std::path::Path;
use tokio::fs;

/// Trait for reading files from different storage backends
#[async_trait]
pub trait FileReader: Send + Sync {
    async fn read_file(&self, path: &Path) -> Result<String>;
    #[allow(dead_code)]
    async fn list_directory(&self, path: &Path) -> Result<Vec<String>>;
    #[allow(dead_code)]
    async fn file_exists(&self, path: &Path) -> bool;
}

/// Local filesystem reader
pub struct LocalFileReader;

impl Default for LocalFileReader {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalFileReader {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl FileReader for LocalFileReader {
    async fn read_file(&self, path: &Path) -> Result<String> {
        let content = fs::read_to_string(path).await?;
        Ok(content)
    }

    async fn list_directory(&self, path: &Path) -> Result<Vec<String>> {
        let mut entries = Vec::new();
        let mut dir_entries = fs::read_dir(path).await?;

        while let Some(entry) = dir_entries.next_entry().await? {
            if let Some(name) = entry.file_name().to_str() {
                entries.push(name.to_string());
            }
        }

        Ok(entries)
    }

    async fn file_exists(&self, path: &Path) -> bool {
        path.exists()
    }
}

#[allow(dead_code)]
pub struct HdfsFileReader {
    client: hdfs_native::Client,
}

impl HdfsFileReader {
    #[allow(dead_code)]
    pub fn new(namenode_url: &str) -> Result<Self> {
        let client = hdfs_native::Client::new(namenode_url)?;
        Ok(Self { client })
    }
}

#[async_trait]
impl FileReader for HdfsFileReader {
    async fn read_file(&self, path: &Path) -> Result<String> {
        let path_str = path.to_string_lossy();
        let mut file = self.client.read(&path_str).await?;
        let file_length = file.file_length();
        let bytes = file.read(file_length).await?;
        Ok(String::from_utf8(bytes.to_vec())?)
    }

    async fn list_directory(&self, path: &Path) -> Result<Vec<String>> {
        let path_str = path.to_string_lossy();
        let entries = self.client.list_status(&path_str, false).await?;
        Ok(entries.into_iter().map(|entry| entry.path).collect())
    }

    async fn file_exists(&self, path: &Path) -> bool {
        let path_str = path.to_string_lossy();
        self.client.get_file_info(&path_str).await.is_ok()
    }
}
