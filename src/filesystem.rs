use crate::blob_container::{BlobContainer, BlobEntry};
use anyhow::Result;
use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request,
};
use libc::{ENOENT, ENOTDIR};
use log::{error, info, warn};
use std::time::Duration;
const TTL: Duration = Duration::from_secs(60); // Cache TTL for file attributes

pub struct BlobFilesystem {
    blob_container: BlobContainer,
    user_id: u32,
    group_id: u32,
}

impl BlobFilesystem {
    pub fn new(blob_container: BlobContainer, user_id: u32, group_id: u32) -> Self {
        Self {
            blob_container,
            user_id,
            group_id,
        }
    }

    /// Get file attributes with intelligent caching and error handling
    fn get_inode_attrs(&self, ino: u64) -> Option<FileAttr> {
        // Find blob by inode and convert to file attributes
        if let Some(blob) = self.blob_container.get_entry_by_inode(ino) {
            Some(self.get_attrs(blob))
        } else {
            warn!("No blob found for inode {ino}");
            None
        }
    }

    fn get_attrs(&self, entry: &BlobEntry) -> FileAttr {
        // Find blob by inode and convert to file attributes
        let mut attr: FileAttr = entry.into();
        attr.uid = self.user_id;
        attr.gid = self.group_id;
        attr
    }
}

impl Filesystem for BlobFilesystem {
    fn getattr(&mut self, _req: &Request, ino: u64, _: Option<u64>, reply: ReplyAttr) {
        info!("getattr(ino={ino})");

        let attr = self.get_inode_attrs(ino);
        match attr {
            Some(attr) => {
                reply.attr(&TTL, &attr);
            }
            None => {
                warn!("Inode {ino} not found");
                reply.error(ENOENT);
            }
        }
    }

    fn init(&mut self, _req: &Request, _: &mut KernelConfig) -> Result<(), i32> {
        info!("Initializing Azure Blob FUSE filesystem...");
        Ok(())
    }

    fn destroy(&mut self) {
        info!("Blob Filesystem destroyed cleanly");
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        info!("readdir(ino={ino}, offset={offset})");

        let entry = self.blob_container.get_directory(ino);
        match entry {
            Some(dir) => {
                let mut current_offset = offset;
                for (name, inode) in dir.entries.iter().skip(offset as usize) {
                    let entry = self.blob_container.get_entry_by_inode(*inode).unwrap();
                    let kind = match entry {
                        BlobEntry::Directory(_) => FileType::Directory,
                        _ => FileType::RegularFile,
                    };

                    let full = reply.add(*inode, current_offset + 1, kind, name);
                    if full {
                        info!("Directory listing buffer full at offset {current_offset}");
                        break;
                    }
                    current_offset += 1;
                }

                reply.ok();
            }
            None => {
                error!("Inode {ino} not found");
                reply.error(ENOTDIR);
            }
        }
    }

    fn lookup(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: ReplyEntry,
    ) {
        let name = name.to_string_lossy();
        info!("lookup(parent={parent}, name={name})");
        let entry = self.blob_container.get_entry_by_inode(parent);
        if let Some(BlobEntry::Directory(dir)) = entry {
            let entry = dir.entries.get(name.as_ref());
            if let Some(&inode) = entry {
                if let Some(blob_entry) = self.blob_container.get_entry_by_inode(inode) {
                    let attrs = self.get_attrs(blob_entry);
                    reply.entry(&TTL, &attrs, 0);
                } else {
                    error!("Blob entry for inode {inode} not found");
                    reply.error(ENOENT);
                }
            } else {
                error!("Entry '{name}' not found in directory inode {parent}");
                reply.error(ENOENT);
            }
        } else {
            error!("Parent inode {parent} is not a directory");
            reply.error(ENOTDIR);
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        info!("read(ino={ino}, offset={offset}, size={size})");
        let result = self.blob_container.download_blob(ino, offset, size);

        match result {
            Ok(data) => {
                reply.data(&data);
            }
            Err(err) => {
                error!("Failed to read blob: {err}");
                reply.error(libc::EIO);
            }
        }
    }
}
