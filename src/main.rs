mod blob_container;
mod filesystem;

use anyhow::{Context, Result};
use azure_core::credentials::TokenCredential;
use azure_identity::DefaultAzureCredential;
use azure_storage_blob::clients::BlobServiceClient;
use clap::Parser;
use essi_ffmpeg::FFmpeg;
use filesystem::BlobFilesystem;
use libc::{getgid, getuid};
use log::info;
use std::{io::Read, path::PathBuf, process::Stdio, sync::Arc};

use crate::blob_container::BlobContainer;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The mountpoint for the filesystem
    #[arg(short, long, default_value = "./mount")]
    mountpoint: PathBuf,

    /// Azure storage account name
    #[arg(short, long)]
    storage_account: String,

    /// Azure blob container name
    #[arg(short, long)]
    container: String,

    /// User ID for filesystem operations
    #[arg(long, default_value_t = get_current_uid())]
    user_id: u32,

    /// Group ID for filesystem operations
    #[arg(long, default_value_t = get_current_gid())]
    group_id: u32,

    #[arg(short, long)]
    input_file: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the logger
    env_logger::init();

    // Parse command line arguments
    let args = Args::parse();

    info!("Starting Azure Blob FUSE filesystem");
    info!("Storage Account: {}", args.storage_account);
    info!("Container: {}", args.container);
    info!("Mount Point: {:?}", args.mountpoint);
    info!(
        "Running as user ID: {}, group ID: {}",
        args.user_id, args.group_id
    );

    // Create Azure credentials using DefaultAzureCredential
    let credential: Arc<dyn TokenCredential> = DefaultAzureCredential::new()?;

    // Create blob service client
    let storage_url = format!("https://{}.blob.core.windows.net", args.storage_account);
    let blob_service_client = BlobServiceClient::new(&storage_url, credential, None)?;
    let container_client = blob_service_client.blob_container_client(args.container);

    // Create filesystem
    let blob_container = BlobContainer::new(container_client).await?;
    let fs = BlobFilesystem::new(blob_container, args.user_id, args.group_id);

    // Mount the filesystem
    let handle = fuser::spawn_mount2(fs, &args.mountpoint, &[])
        .with_context(|| format!("Failed to mount filesystem at {:?}", args.mountpoint))?;

    let input_file = args.mountpoint.join(&args.input_file);
    analyze_file(input_file)
        .with_context(|| format!("Failed to analyze file: {:?}", args.input_file))?;

    drop(handle);
    info!("Filesystem unmounted cleanly.");
    Ok(())
}

fn get_current_uid() -> u32 {
    unsafe { getuid() }
}

fn get_current_gid() -> u32 {
    unsafe { getgid() }
}

/// Analyze a file using `essi-ffmpeg` and print its metadata
fn analyze_file(file_path: PathBuf) -> Result<()> {
    let mut command = FFmpeg::new_with_program("ffprobe")
        .arg("-hide_banner")
        .input_with_file(file_path)
        .args(["-of", "json"])
        .arg("-show_format")
        .done()
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .start()?;

    let mut stdout = String::with_capacity(1024);
    let mut stderr = String::with_capacity(1024);
    let _ = command.take_stdout().unwrap().read_to_string(&mut stdout)?;
    let _ = command.take_stderr().unwrap().read_to_string(&mut stderr)?;
    let status = command.wait()?;
    info!(
        "FFmpeg command exited with status: {status} out: {stdout} \n err: {stderr}");
    Ok(())
}
