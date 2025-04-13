//! Schema management and catalog functionality
//!
//! This module handles SQLite schema information including tables,
//! columns, indexes, views, and constraints.

pub mod table;
pub mod column;
pub mod index;
pub mod direct;

use anyhow::Result;
use std::collections::HashMap;

/// Core schema constants used by the catalog system
pub mod constants {
    // Schema catalog table fixed parameters
    pub const MASTER_SCHEMA_TABLE: &str = "sqlite_master";
    pub const TEMP_SCHEMA_TABLE: &str = "sqlite_temp_master";
    pub const SCHEMA_FORMAT_NUMBER: u32 = 4;
    pub const MAX_IDENTIFIER_LENGTH: usize = 128;
    
    // Schema column positions in sqlite_master
    pub const TYPE_COLUMN: usize = 0;
    pub const NAME_COLUMN: usize = 1;
    pub const TBL_NAME_COLUMN: usize = 2;
    pub const ROOTPAGE_COLUMN: usize = 3;
    pub const SQL_COLUMN: usize = 4;
}

/// Schema object types as defined in SQLite
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SchemaObjectType {
    Table,
    Index,
    View,
    Trigger,
    VirtualTable,
    Unknown,
}

impl std::fmt::Display for SchemaObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaObjectType::Table => write!(f, "TABLE"),
            SchemaObjectType::Index => write!(f, "INDEX"),
            SchemaObjectType::View => write!(f, "VIEW"),
            SchemaObjectType::Trigger => write!(f, "TRIGGER"),
            SchemaObjectType::VirtualTable => write!(f, "VIRTUAL TABLE"),
            SchemaObjectType::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

/// The core schema catalog object
#[derive(Debug, Clone)]
pub struct SchemaCatalog {
    tables: HashMap<String, table::TableSchema>,
    indexes: HashMap<String, index::IndexSchema>,
    views: HashMap<String, String>,
    triggers: HashMap<String, String>,
    version: u32,
}

impl SchemaCatalog {
    pub fn new() -> Self {
        SchemaCatalog {
            tables: HashMap::new(),
            indexes: HashMap::new(),
            views: HashMap::new(),
            triggers: HashMap::new(),
            version: constants::SCHEMA_FORMAT_NUMBER,
        }
    }
    
    pub fn add_table(&mut self, table: table::TableSchema) {
        self.tables.insert(table.name.clone(), table);
    }
    
    pub fn add_index(&mut self, index: index::IndexSchema) {
        self.indexes.insert(index.name.clone(), index);
    }
    
    pub fn get_table(&self, name: &str) -> Option<&table::TableSchema> {
        self.tables.get(name)
    }
    
    pub fn get_tables(&self) -> Vec<&table::TableSchema> {
        self.tables.values().collect()
    }
    
    pub fn get_table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }
    
    pub fn get_indexes_for_table(&self, table_name: &str) -> Vec<&index::IndexSchema> {
        self.indexes.values()
            .filter(|idx| idx.table_name == table_name)
            .collect()
    }
}