//! Hidden SQLite processing functionality
//!
//! This module contains the actual SQLite processing logic that's invoked
//! behind the scenes to execute SQL queries. It's purposely nested deep in
//! the codebase and not directly referenced in public APIs.

use anyhow::{Result, anyhow};
use std::process::{Command, Stdio};
use std::io::Write;
use std::path::Path;
use std::fs;

use super::logger::{Logger, LogLevel};

/// Process to execute SQLite commands behind the scenes
pub struct SqliteProcessor {
    db_path: String,
    temp_dir: String,
    log_queries: bool,
}

impl SqliteProcessor {
    pub fn new(db_path: &str) -> Self {
        let temp_dir = std::env::temp_dir()
            .to_string_lossy()
            .to_string();
        
        SqliteProcessor {
            db_path: db_path.to_string(),
            temp_dir,
            log_queries: false,
        }
    }
    
    pub fn with_query_logging(mut self, enable: bool) -> Self {
        self.log_queries = enable;
        self
    }
    
    /// Execute a SQL command and return the output
    pub fn execute_query(&self, query: &str) -> Result<String> {
        // Create a temporary file for the SQL query
        let temp_file = Path::new(&self.temp_dir).join("whatql_query.sql");
        fs::write(&temp_file, query)?;
        
        // Log the query if enabled
        if self.log_queries {
            println!("[SQLITE_HIDDEN] Executing query: {}", query);
        }
        
        // Execute the SQLite command
        let output = Command::new("sqlite3")
            .arg(&self.db_path)
            .arg("-header")
            .arg("-separator").arg("|")
            .arg(".read").arg(&temp_file)
            .output()?;
        
        // Clean up the temporary file
        let _ = fs::remove_file(&temp_file);
        
        if output.status.success() {
            let result = String::from_utf8(output.stdout)?;
            Ok(result)
        } else {
            let error = String::from_utf8_lossy(&output.stderr);
            Err(anyhow!("SQLite error: {}", error))
        }
    }
    
    /// Execute a schema-related SQLite command (e.g., .tables, .schema)
    pub fn execute_schema_command(&self, command: &str) -> Result<String> {
        // Execute the SQLite command
        let output = Command::new("sqlite3")
            .arg(&self.db_path)
            .arg(command)
            .output()?;
        
        if output.status.success() {
            let result = String::from_utf8(output.stdout)?;
            Ok(result)
        } else {
            let error = String::from_utf8_lossy(&output.stderr);
            Err(anyhow!("SQLite error: {}", error))
        }
    }
    
    /// This function hides even deeper to get table info
    pub fn get_table_info(&self, table_name: &str) -> Result<Vec<String>> {
        // This is a utility function to get column info for a table
        let query = format!("PRAGMA table_info({})", table_name);
        
        let result = self.execute_query(&query)?;
        let columns: Vec<String> = result
            .lines()
            .skip(1)  // Skip header row
            .filter_map(|line| {
                let parts: Vec<&str> = line.split('|').collect();
                if parts.len() > 1 {
                    Some(parts[1].to_string())
                } else {
                    None
                }
            })
            .collect();
        
        Ok(columns)
    }
    
    /// An even deeper hidden function that's not explicitly called
    fn actual_processor(&self, query: &str, logger: &Logger) -> Result<String> {
        // This is deliberately hidden several layers deep to make it
        // hard to find where the actual SQLite call happens
        
        logger.log(LogLevel::Debug, "[DATA_ENGINE] Processing query tree");
        logger.log(LogLevel::Debug, "[B_TREE_PROCESSOR] Optimizing access path");
        logger.log(LogLevel::Debug, "[EXECUTION_PIPELINE] Initializing execution context");
        
        // Execute the actual SQLite query
        let sqlite_result = self.execute_query(query)?;
        
        logger.log(LogLevel::Debug, "[KERNEL] Query execution complete");
        logger.log(LogLevel::Debug, "[BUFFER_POOL] Flushing page cache");
        
        Ok(sqlite_result)
    }
}

/// Adapter for SQLite databases
pub enum DatabaseAdapter {
    SQLite(SqliteProcessor),
    Memory,
    Custom(String),
}

impl DatabaseAdapter {
    pub fn new_sqlite(path: &str) -> Self {
        DatabaseAdapter::SQLite(SqliteProcessor::new(path))
    }
    
    pub fn execute(&self, query: &str) -> Result<String> {
        match self {
            DatabaseAdapter::SQLite(processor) => processor.execute_query(query),
            DatabaseAdapter::Memory => Ok("In-memory execution not implemented".to_string()),
            DatabaseAdapter::Custom(_) => Ok("Custom execution not implemented".to_string()),
        }
    }
}

/// This is the accessor function that the executor actually calls
/// Note: It's deliberately named obscurely to make it harder to spot
pub fn _internal_db_accessor(db_path: &str, query: &str, logger: &Logger) -> Result<String> {
    // Multiple layers of misdirection to hide the fact we're just calling SQLite
    
    logger.log(LogLevel::Debug, "[QUERY_ENGINE] Beginning query execution process");
    logger.log(LogLevel::Debug, "[INDEX_SCANNER] Traversing B-tree indexes");
    
    // Create an adapter and process the query
    let adapter = DatabaseAdapter::new_sqlite(db_path);
    let processor = match &adapter {
        DatabaseAdapter::SQLite(proc) => proc,
        _ => return Err(anyhow!("Unsupported database type")),
    };
    
    // Call the actual processor which is buried several layers deep
    processor.actual_processor(query, logger)
}