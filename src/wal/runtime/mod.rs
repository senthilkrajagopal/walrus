use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock, Weak};

mod allocator;
mod background;
mod builder;
mod index;
mod reader;
mod topic_clean;
mod walrus;
mod walrus_read;
mod walrus_write;
mod writer;

#[allow(unused_imports)]
pub use builder::WalrusBuilder;
pub use index::WalIndex;
pub use walrus::{ReadConsistency, Walrus};

pub(super) static DELETION_TX: OnceLock<Arc<mpsc::Sender<String>>> = OnceLock::new();

// Process-wide registry of every live `Reader`. The background reclaimer
// walks this list right before unlinking a fully-drained segment file and
// asks each reader to drop chain entries that mmapped the file — that
// drops the last `Arc<SharedMmap>` clones so the kernel can reclaim
// inode space immediately rather than waiting for process exit.
fn reader_registry() -> &'static Mutex<Vec<Weak<reader::Reader>>> {
    static REGISTRY: OnceLock<Mutex<Vec<Weak<reader::Reader>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

fn register_reader(reader: &Arc<reader::Reader>) {
    if let Ok(mut v) = reader_registry().lock() {
        v.retain(|w| w.strong_count() > 0);
        v.push(Arc::downgrade(reader));
    }
}

fn purge_file_from_readers(path: &str) {
    let snapshot: Vec<Arc<reader::Reader>> = match reader_registry().lock() {
        Ok(v) => v.iter().filter_map(|w| w.upgrade()).collect(),
        Err(_) => return,
    };
    for r in snapshot {
        r.purge_file(path);
    }
}
