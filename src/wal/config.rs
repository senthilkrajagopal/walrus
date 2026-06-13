use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::SystemTime;

// Global flag to choose backend
pub(crate) static USE_FD_BACKEND: AtomicBool = AtomicBool::new(true);

// Public function to enable FD backend
pub fn enable_fd_backend() {
    USE_FD_BACKEND.store(true, Ordering::Relaxed);
}

// Public function to disable FD backend (use mmap instead)
pub fn disable_fd_backend() {
    USE_FD_BACKEND.store(false, Ordering::Relaxed);
}

// Macro to conditionally print debug messages
macro_rules! debug_print {
    ($($arg:tt)*) => {
        if std::env::var("WALRUS_QUIET").is_err() {
            println!($($arg)*);
        }
    };
}

pub(crate) use debug_print;

#[derive(Clone, Copy, Debug)]
pub enum FsyncSchedule {
    Milliseconds(u64),
    SyncEach, // fsync after every single entry
    NoFsync,  // disable fsyncing entirely (maximum throughput, no durability)
}

pub(crate) const DEFAULT_BLOCK_SIZE: u64 = 10 * 1024 * 1024; // 10mb
// Segment files roll over at DEFAULT_BLOCK_SIZE * BLOCKS_PER_FILE. Each new
// file is `set_len`'d to MAX_FILE_SIZE up front (sparse on ext4/xfs, so the
// apparent size is the full file but real disk = only what's written). A file
// is reclaimed only once ALL its blocks are checkpointed, so the file size is
// the WAL's reclamation granularity AND its apparent-size floor per topic.
// 10 blocks → 100 MiB files: tuned down from 100 (1 GiB) for snowmelt's
// moderate per-partition ingest — keeps the apparent footprint small (a 4-pod
// box showed 4 GiB apparent / ~0.5 GiB real with 1 GiB files) and lets the WAL
// reclaim space at a finer grain. 10 MiB blocks are unchanged (they bound
// batch-read planning). NOTE: changing MAX_FILE_SIZE is NOT compatible with an
// existing WAL on disk — recovery scans each file up to MAX_FILE_SIZE, so old
// 1 GiB files would be mis-read. Wipe the WAL (clean redeploy / PVC reset)
// when changing this.
pub(crate) const BLOCKS_PER_FILE: u64 = 10;
pub(crate) const MAX_ALLOC: u64 = 1 * 1024 * 1024 * 1024; // 1 GiB cap per block
// Expose so integration tests can match the on-disk layout when poking raw files.
pub const PREFIX_META_SIZE: usize = 256;
pub(crate) const MAX_FILE_SIZE: u64 = DEFAULT_BLOCK_SIZE * BLOCKS_PER_FILE;
// Max entries returned by a single `batch_read_for_topic` (and accepted by a
// single `batch_append_for_topic`). Raised from 2000 → 200_000 so a consumer
// can drain a large batch in one read: snowmelt's dp-flush does a loss-free
// peek(checkpoint=false) → vortex-write → consume(checkpoint=true) per read,
// and a small cap forced one vortex file per 2000 records, fragmenting storage
// and slowing queries. 200k entries ≈ a few MB at typical record sizes.
pub(crate) const MAX_BATCH_ENTRIES: usize = 200_000;
pub(crate) const MAX_BATCH_BYTES: u64 = 10 * 1024 * 1024 * 1024; // 10 GiB total payload limit

static LAST_MILLIS: AtomicU64 = AtomicU64::new(0);

pub(crate) fn now_millis_str() -> String {
    let system_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_millis();

    let mut observed = LAST_MILLIS.load(Ordering::Relaxed);
    loop {
        let system_ms_u64 = system_ms.try_into().unwrap_or(u64::MAX);
        let candidate = if system_ms_u64 <= observed {
            observed.saturating_add(1)
        } else {
            system_ms_u64
        };

        match LAST_MILLIS.compare_exchange(observed, candidate, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => return candidate.to_string(),
            Err(actual) => observed = actual,
        }
    }
}

pub(crate) fn checksum64(data: &[u8]) -> u64 {
    // FNV-1a 64-bit checksum
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut hash = FNV_OFFSET;
    for &b in data {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

pub(crate) fn wal_data_dir() -> PathBuf {
    std::env::var_os("WALRUS_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("wal_files"))
}

pub(crate) fn sanitize_namespace(key: &str) -> String {
    let mut sanitized: String = key
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();

    if sanitized.trim_matches('_').is_empty() {
        sanitized = format!("ns_{:x}", checksum64(key.as_bytes()));
    }
    sanitized
}
