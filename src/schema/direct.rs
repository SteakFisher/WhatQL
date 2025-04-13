use anyhow::Result;
use rusqlite::Connection;

pub struct DirectQueryInfo {
    pub table_names: Vec<String>,
    pub column_names: Vec<String>,
}

pub fn extract_query_info(db_path: &str, query: &str) -> Result<DirectQueryInfo> {
    // Connect to the database
    let conn = Connection::open(db_path)?;
    
    // Prepare statement with our query but don't execute it
    let stmt = conn.prepare(query)?;
    
    // Get column names directly from statement
    let mut column_names = Vec::new();
    for idx in 0..stmt.column_count() {
        // Fix: handle the Result properly
        if let Ok(name) = stmt.column_name(idx) {
            column_names.push(name.to_string());
        }
    }
    
    // For tables, get them from sqlite_master
    let mut table_names = Vec::new();
    let table_query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'";
    let mut tables_stmt = conn.prepare(table_query)?;
    let table_rows = tables_stmt.query_map([], |row| row.get::<_, String>(0))?;
    
    for table_result in table_rows {
        table_names.push(table_result?);
    }
    
    Ok(DirectQueryInfo {
        table_names,
        column_names,
    })
}