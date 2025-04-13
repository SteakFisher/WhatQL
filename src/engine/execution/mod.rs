pub mod planner;
pub mod executor;
pub mod optimizer;

use std::fmt;

/// Types of execution operations
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionOperationType {
    TableScan,
    IndexScan,
    NestedLoopJoin,
    HashJoin,
    MergeJoin,
    Filter,
    Projection,
    Sort,
    HashAggregate,
    GroupAggregate,
    Limit,
}

/// Various join strategies available to the query optimizer
#[derive(Debug, Clone, PartialEq)]
pub enum JoinStrategy {
    NestedLoop,
    Hash,
    Merge,
    Index,
}

/// Result row for query execution
#[derive(Debug, Clone)]
pub struct ResultRow {
    values: Vec<ColumnValue>,
}

impl ResultRow {
    pub fn new(values: Vec<ColumnValue>) -> Self {
        ResultRow { values }
    }
    
    pub fn get_values(&self) -> &Vec<ColumnValue> {
        &self.values
    }
}

impl fmt::Display for ResultRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self.values.iter()
            .map(|v| v.to_string())
            .collect();
        write!(f, "{}", parts.join("|"))
    }
}

/// Value type for a column in a result row
#[derive(Debug, Clone)]
pub enum ColumnValue {
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Null,
}

impl fmt::Display for ColumnValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnValue::Integer(i) => write!(f, "{}", i),
            ColumnValue::Real(r) => write!(f, "{}", r),
            ColumnValue::Text(s) => write!(f, "{}", s),
            ColumnValue::Blob(b) => write!(f, "[BLOB {}B]", b.len()),
            ColumnValue::Null => write!(f, "NULL"),
        }
    }
}

/// Statistics used for query planning and cost estimation
#[derive(Debug, Clone, Default)]
pub struct TableStatistics {
    pub table_name: String,
    pub row_count: usize,
    pub page_count: usize,
    pub avg_row_size: usize,
    pub columns: Vec<ColumnStatistics>,
}

#[derive(Debug, Clone, Default)]
pub struct ColumnStatistics {
    pub name: String,
    pub distinct_values: usize,
    pub null_count: usize,
    pub min_value: Option<ColumnValue>,
    pub max_value: Option<ColumnValue>,
    pub has_index: bool,
}