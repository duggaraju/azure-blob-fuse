use anyhow::{Context, Result};
use azure_core::Bytes;
use azure_core::time::OffsetDateTime;
use azure_storage_blob::BlobContainerClient;
use fuser::{FUSE_ROOT_ID, FileAttr};
use futures::StreamExt;
use log::{error, info};
use std::collections::HashMap;
use std::path::Path;
use std::time::{Instant, SystemTime};

/// Represents a blob item in the Azure Storage container
#[derive(Debug, Clone)]
pub struct BlobInfo {
    pub name: String,
    pub size: u64,
    pub last_modified: SystemTime,
    pub inode: u64,
    pub data: Option<Bytes>, // Optional data for the blob, can be used for caching
}

impl BlobInfo {
    pub fn new(name: String, size: u64, last_modified: SystemTime, inode: u64) -> Self {
        Self {
            name,
            size,
            last_modified,
            inode,
            data: None, // Data can be set later if needed
        }
    }

    async fn download(&mut self, client: &BlobContainerClient) -> Result<Bytes> {
        match self.data {
            Some(ref data) => Ok(data.clone()),
            None => {
                let data = client
                    .blob_client(self.name.clone())
                    .download(None)
                    .await
                    .context(format!("Failed to download blob: {}", self.name))?
                    .into_raw_body()
                    .collect()
                    .await?;
                self.data = Some(data.clone());
                Ok(data)
            }
        }
    }

    /// Synchronous method to download blob content
    pub fn download_sync(&mut self, client: &BlobContainerClient) -> Result<Bytes> {
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(self.download(client))
    }
}

pub struct BlobDirectory {
    pub entries: HashMap<String, u64>,
    pub inode: u64,
}

impl BlobDirectory {
    pub fn new(inode: u64, parent: u64) -> Self {
        Self {
            entries: HashMap::from([("..".to_string(), parent), (".".to_string(), inode)]),
            inode,
        }
    }

    /// Adds a file to the directory
    pub fn add_file(&mut self, name: String, inode: u64) {
        self.entries.insert(name, inode);
    }

    /// Checks if the directory is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn root() -> Self {
        Self {
            entries: HashMap::from([
                ("..".to_string(), FUSE_ROOT_ID),
                (".".to_string(), FUSE_ROOT_ID),
            ]),
            inode: FUSE_ROOT_ID,
        }
    }
}

pub enum BlobEntry {
    File(BlobInfo),
    Directory(BlobDirectory), // Represents a directory entry
}

impl From<&BlobEntry> for FileAttr {
    fn from(entry: &BlobEntry) -> Self {
        match entry {
            BlobEntry::File(blob) => FileAttr {
                ino: blob.inode,
                size: blob.size,
                blocks: blob.size.div_ceil(512), // 512 bytes per block
                atime: blob.last_modified,
                mtime: blob.last_modified,
                ctime: blob.last_modified,
                crtime: blob.last_modified,
                kind: fuser::FileType::RegularFile,
                perm: 0o644, // rw-r--r--
                nlink: 1,    // Regular files have one link
                uid: 0,      // Owner user ID (can be set to actual user ID)
                gid: 0,
                rdev: 0,
                blksize: 4096,
                flags: 0, // Owner group ID (can be set to actual group ID)
            },
            BlobEntry::Directory(dir) => FileAttr {
                ino: dir.inode, // Directories can have a special inode or use a fixed one
                size: 0,        // Size is not meaningful for directories
                blocks: 0,      // No blocks for empty directories
                atime: SystemTime::now(),
                mtime: SystemTime::now(),
                ctime: SystemTime::now(),
                crtime: SystemTime::now(),
                kind: fuser::FileType::Directory,
                perm: 0o755, // rwxr-xr-x for directories
                nlink: 2,    // Directories have at least two links (., ..)
                uid: 0,      // Owner user ID (can be set to actual user ID)
                gid: 0,      // Owner group ID (can be set to actual group ID)
                rdev: 0,
                blksize: 4096,
                flags: 0, // Owner group ID (can be set to actual group ID)
            },
        }
    }
}

/// Azure blob container wrapper that handles blob operations and caching
pub struct BlobContainer {
    container_client: BlobContainerClient,
    // Cache for blob metadata to avoid repeated API calls
    blob_cache: HashMap<String, BlobEntry>,
    inode_map: HashMap<u64, String>,
    next_inode: u64,
}

impl BlobContainer {
    /// Creates a new BlobContainer instance
    pub async fn new(container_client: BlobContainerClient) -> Result<Self> {
        let inode_map = HashMap::from([(FUSE_ROOT_ID, String::new())]);
        let blob_cache =
            HashMap::from([(String::new(), BlobEntry::Directory(BlobDirectory::root()))]);

        let mut container = Self {
            container_client,
            blob_cache,
            inode_map,
            next_inode: 2, // Start from 2, as 1 is reserved for root
        };
        container.load_blobs().await?;
        Ok(container)
    }

    fn add_directory(&mut self, name: String, inode: u64, parent: u64) {
        let directory = BlobDirectory::new(inode, parent);
        self.blob_cache
            .insert(name.clone(), BlobEntry::Directory(directory));
        self.inode_map.insert(inode, name.clone());

        self.inode_map
            .get_mut(&parent)
            .and_then(|parent_name| self.blob_cache.get_mut(parent_name))
            .and_then(|entry| {
                if let BlobEntry::Directory(dir) = entry {
                    dir.add_file(name, inode);
                    Some(())
                } else {
                    None
                }
            });
    }

    /// Refreshes the blob cache by listing all blobs in the container
    async fn load_blobs(&mut self) -> anyhow::Result<()> {
        info!("Refreshing blob cache from Azure Storage (cache expired or empty)");

        // Record the start time of cache refresh
        let refresh_start = Instant::now();

        // List all blobs in the container
        let mut page_stream = self.container_client.list_blobs(None)?;

        while let Some(page_result) = page_stream.next().await {
            match page_result {
                Ok(page) => {
                    let segment = page.into_body().await?.segment;
                    for blob_item in segment.blob_items {
                        let blob_name = &blob_item.name.unwrap().content.unwrap();
                        info!("Processing entry: {blob_name}");
                        let path = Path::new(&blob_name);
                        let parent_path = if let Some(p) = path.parent() {
                            self.process_directories(blob_name);
                            p.to_str().unwrap_or("")
                        } else {
                            ""
                        };

                        let mut size: u64 = 0;
                        let mut last_modified: SystemTime = SystemTime::now();
                        if let Some(properties) = &blob_item.properties {
                            size = properties.content_length.unwrap_or(0);
                            last_modified = SystemTime::from(
                                properties
                                    .last_modified
                                    .unwrap_or(OffsetDateTime::now_utc()),
                            );
                        }

                        // Create blob entry
                        let inode = self.next_inode;
                        self.next_inode += 1;

                        let blob_info = BlobInfo {
                            name: blob_name.clone(),
                            size,
                            last_modified,
                            inode,
                            data: None,
                        };

                        self.inode_map.insert(inode, blob_name.clone());
                        self.blob_cache
                            .insert(blob_name.clone(), BlobEntry::File(blob_info));
                        let parent = self.blob_cache.get_mut(parent_path);
                        if let Some(BlobEntry::Directory(parent_dir)) = parent {
                            let name = path.file_name().unwrap_or_default().to_string_lossy();
                            parent_dir.add_file(name.to_string(), inode);
                        }
                    }
                }
                Err(e) => {
                    error!("Error listing blobs: {e}");
                    return Err(e).context("Failed to list blobs from Azure Storage");
                }
            }
        }

        let refresh_duration = refresh_start.elapsed();
        info!(
            "Blob cache refreshed with {} entries ({} blobs, {} directories) in {:.2}s",
            self.blob_cache.len(),
            self.blob_cache
                .values()
                .filter(|b| matches!(b, BlobEntry::File(_)))
                .count(),
            self.blob_cache
                .values()
                .filter(|b| matches!(b, BlobEntry::Directory(_)))
                .count(),
            refresh_duration.as_secs_f64()
        );
        Ok(())
    }

    /// Processes directories for a given blob name, creating directory entries as needed
    pub fn process_directories(&mut self, blob_name: &str) {
        let mut parent_inode = FUSE_ROOT_ID;
        let path_parts: Vec<&str> = blob_name.split('/').collect();
        for i in 1..path_parts.len() {
            let dir_path = path_parts[..i].join("/");
            let entry = self.blob_cache.get(&dir_path);
            if let Some(BlobEntry::Directory(dir)) = entry {
                parent_inode = dir.inode;
            } else {
                self.add_directory(dir_path, self.next_inode, parent_inode);
                parent_inode = self.next_inode;
                self.next_inode += 1;
            }
        }
    }

    /// Gets blob info by inode
    pub fn get_entry_by_inode(&self, inode: u64) -> Option<&BlobEntry> {
        self.inode_map
            .get(&inode)
            .and_then(|path| self.blob_cache.get(path))
    }

    pub fn get_directory(&self, inode: u64) -> Option<&BlobDirectory> {
        self.get_entry_by_inode(inode)
            .and_then(|entry| match entry {
                BlobEntry::Directory(dir) => Some(dir),
                _ => None,
            })
    }

    /// Downloads blob content
    pub fn download_blob(&mut self, inode: u64, offset: i64, size: u32) -> Result<Bytes> {
        info!("Downloading blob: {inode} {offset} {size}");
        let entry = self
            .inode_map
            .get(&inode)
            .and_then(|blob_name| self.blob_cache.get_mut(blob_name));

        if let Some(BlobEntry::File(blob)) = entry {
            let data = blob.download_sync(&self.container_client)?;
            let end = (offset as usize + size as usize).min(data.len());
            Ok(data.slice(offset as usize..end))
        } else {
            Err(anyhow::format_err!("Blob with inode {} not found", inode))
        }
    }

    /// Debug function to print the entries in the blob_cache in detail
    pub fn debug_blob_cache(&self) {
        info!("Debugging blob_cache entries:");
        info!("inode map: {:?}", self.inode_map);
        for (path, entry) in &self.blob_cache {
            match entry {
                BlobEntry::File(blob) => {
                    info!(
                        "File: Path: {}, Inode: {}, Size: {}, Last Modified: {:?}",
                        path, blob.inode, blob.size, blob.last_modified
                    );
                }
                BlobEntry::Directory(dir) => {
                    info!(
                        "Directory: Path: {}, Inode: {}, Entries: {:?}",
                        path,
                        dir.inode,
                        dir.entries.keys().collect::<Vec<_>>()
                    );
                }
            }
        }
    }
}
