//! Column schema definition and management

use std::fmt;

/// Affinity types as defined in SQLite
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnAffinity {
    Text,
    Numeric,
    Integer,
    Real,
    Blob,
    None,
}

impl fmt::Display for ColumnAffinity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnAffinity::Text => write!(f, "TEXT"),
            ColumnAffinity::Numeric => write!(f, "NUMERIC"),
            ColumnAffinity::Integer => write!(f, "INTEGER"),
            ColumnAffinity::Real => write!(f, "REAL"),
            ColumnAffinity::Blob => write!(f, "BLOB"),
            ColumnAffinity::None => write!(f, "NONE"),
        }
    }
}

/// Constraint types for columns
#[derive(Debug, Clone, PartialEq)]
pub enum ConstraintType {
    NotNull,
    PrimaryKey,
    Unique,
    ForeignKey { table: String, column: String },
    Check { expression: String },
    Default { value: String },
    Collate { collation: String },
}

/// Schema information for a database column
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
    pub position: usize,
    pub is_nullable: bool,
    pub default_value: Option<String>,
    pub is_primary_key: bool,
}

impl ColumnSchema {
    /// Determine column affinity based on data type
    pub fn get_affinity(&self) -> ColumnAffinity {
        // Implementation of SQLite affinity rules:
        // https://www.sqlite.org/datatype3.html#affinity
        
        let upper_type = self.data_type.to_uppercase();
        
        if upper_type.contains("INT") {
            ColumnAffinity::Integer
        } else if upper_type.contains("CHAR") || 
                  upper_type.contains("CLOB") || 
                  upper_type.contains("TEXT") {
            ColumnAffinity::Text
        } else if upper_type.contains("BLOB") || self.data_type.is_empty() {
            ColumnAffinity::Blob
        } else if upper_type.contains("REAL") || 
                  upper_type.contains("FLOA") || 
                  upper_type.contains("DOUB") {
            ColumnAffinity::Real
        } else {
            ColumnAffinity::Numeric
        }
    }
    
    /// Calculate storage requirements for this column type
    pub fn estimate_storage_size(&self) -> usize {
        match self.get_affinity() {
            ColumnAffinity::Integer => 8,  // 64-bit integer
            ColumnAffinity::Real => 8,     // 64-bit float
            ColumnAffinity::Text => 32,    // Reasonable average for text
            ColumnAffinity::Blob => 100,   // Arbitrary average for blobs
            ColumnAffinity::Numeric => 8,  // Typically stored as 64-bit
            ColumnAffinity::None => 0,     // No storage
        }
    }
    
    /// Check if the column can be indexed efficiently
    pub fn is_indexable(&self) -> bool {
        // Most columns are indexable, except for large BLOBs
        self.get_affinity() != ColumnAffinity::Blob || 
            self.estimate_storage_size() < 1000
    }
    
    /// Get SQL definition for this column
    pub fn get_sql_definition(&self) -> String {
        let mut sql = format!("{} {}", self.name, self.data_type);
        
        if self.is_primary_key {
            sql.push_str(" PRIMARY KEY");
        }
        
        if !self.is_nullable {
            sql.push_str(" NOT NULL");
        }
        
        if let Some(default) = &self.default_value {
            sql.push_str(&format!(" DEFAULT {}", default));
        }
        
        sql
    }
}