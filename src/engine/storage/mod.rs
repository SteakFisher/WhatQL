pub mod binary;
pub mod page_manager;
pub mod varint;

// Storage format constants
pub const PAGE_HEADER_SIZE: usize = 8;
pub const CELL_POINTER_SIZE: usize = 2;
pub const OVERFLOW_PAGE_HEADER: usize = 4;
pub const FREELIST_LEAF_SIZE: usize = 4;
pub const MIN_EMBEDDED_FRAC: u8 = 32;
pub const LEAF_PAYLOAD_FRAC: u8 = 32;

#[derive(Debug, Clone, PartialEq)]
pub enum StorageError {
    IoError(String),
    InvalidFormat(String),
    CorruptPage(String),
    PageNotFound(usize),
    OutOfBounds(String),
    HeaderMismatch(String),
}

/// Database page types as defined in SQLite format
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageType {
    InteriorIndex = 0x02,
    InteriorTable = 0x05,
    LeafIndex = 0x0A,
    LeafTable = 0x0D,
    Overflow = 0xFF,
    FreelistTrunk = 0xFE,
    FreelistLeaf = 0xFD,
    Unknown = 0x00,
}

impl From<u8> for PageType {
    fn from(value: u8) -> Self {
        match value {
            0x02 => PageType::InteriorIndex,
            0x05 => PageType::InteriorTable,
            0x0A => PageType::LeafIndex,
            0x0D => PageType::LeafTable,
            0xFF => PageType::Overflow,
            0xFE => PageType::FreelistTrunk,
            0xFD => PageType::FreelistLeaf,
            _ => PageType::Unknown,
        }
    }
}

/// Low-level page format control flags
pub struct PageFlags {
    pub encoding_format: u8,
    pub has_overflow: bool,
    pub leaf_page: bool,
    pub uses_freelist: bool,
    pub btree_version: u8,
}

/// Various file format versions used by SQLite
pub enum FileFormatVersion {
    Legacy = 1,     // Original format
    WAL = 2,        // Write-Ahead Logging
    Incremental = 3, // Support for incremental vacuum
    Modern = 4,      // Current version
}