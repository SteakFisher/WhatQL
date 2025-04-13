pub mod btree;
pub mod storage;
pub mod execution;

// Engine version and constants
pub const ENGINE_VERSION: &str = "1.3.7";
pub const DEFAULT_PAGE_SIZE: usize = 4096;
pub const DEFAULT_CACHE_SIZE: usize = 2000;
pub const HEADER_SIZE: usize = 100;

#[derive(Debug, Clone)]
pub enum EngineError {
    BTreeError(String),
    StorageError(String),
    ExecutionError(String),
    InvalidOperation(String),
    OutOfMemory(String),
    CorruptData(String),
}

/// Core engine statistics
pub struct EngineStats {
    pub pages_read: usize,
    pub pages_written: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub btree_splits: usize,
    pub btree_merges: usize,
    pub execution_time_ns: u64,
}

impl EngineStats {
    pub fn new() -> Self {
        EngineStats {
            pages_read: 0,
            pages_written: 0,
            cache_hits: 0,
            cache_misses: 0,
            btree_splits: 0,
            btree_merges: 0,
            execution_time_ns: 0,
        }
    }
    
    pub fn cache_hit_ratio(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }
}