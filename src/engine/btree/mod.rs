pub mod node;
pub mod page_cache;
pub mod traversal;

use std::fmt;

// B-tree specific constants
pub const MAX_LEAF_PAYLOAD: usize = 2000;
pub const BTREE_HEADER_SIZE: usize = 12;
pub const DEFAULT_FILL_FACTOR: f64 = 0.7;
pub const MIN_KEYS_PER_INTERNAL_PAGE: usize = 2;

#[derive(Debug, Clone, PartialEq)]
pub enum BTreeNodeType {
    Internal,
    Leaf,
    Overflow,
    FreeList,
}

/// Represents the current state of a B-tree
#[derive(Debug)]
pub struct BTreeState {
    pub node_count: usize,
    pub leaf_count: usize,
    pub internal_count: usize,
    pub overflow_count: usize,
    pub free_pages: usize,
    pub depth: usize,
    pub root_page: usize,
}


#[derive(Debug, Clone, PartialEq)]
pub enum BTreeError {
    InvalidNodeType,
    PageNotFound(usize),
    KeyNotFound(Vec<u8>),
    DuplicateKey(Vec<u8>),
    InvalidFormat(String),
    IOError(String),
}
impl fmt::Display for BTreeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BTreeError::InvalidNodeType => write!(f, "Invalid node type"),
            BTreeError::PageNotFound(page_id) => write!(f, "Page not found: {}", page_id),
            BTreeError::KeyNotFound(key) => write!(f, "Key not found: {:?}", key),
            BTreeError::DuplicateKey(key) => write!(f, "Duplicate key: {:?}", key),
            BTreeError::InvalidFormat(msg) => write!(f, "Invalid format: {}", msg),
            BTreeError::IOError(msg) => write!(f, "IO error: {}", msg),
        }
    }
}

/// The primary B-tree structure used by the engine
pub struct BTree {
    pub root_page_id: usize,
    pub page_size: usize,
    pub depth: usize,
    pub key_count: usize,
    pub is_unique: bool,
}

impl BTree {
    pub fn new(root_page_id: usize, page_size: usize) -> Self {
        BTree {
            root_page_id,
            page_size,
            depth: 1,
            key_count: 0,
            is_unique: true,
        }
    }
    
    pub fn get_state(&self) -> BTreeState {
        BTreeState {
            node_count: 0, // Would be calculated from the tree
            leaf_count: 0,
            internal_count: 0,
            overflow_count: 0,
            free_pages: 0,
            depth: self.depth,
            root_page: self.root_page_id,
        }
    }
}