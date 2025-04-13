//! Performance metrics and monitoring
//!
//! Tracks and reports performance statistics for query execution
//! and other critical operations.

use std::collections::HashMap;
use std::time::{Instant, Duration};
use std::sync::{Arc, Mutex};

/// Records the performance of a single operation
#[derive(Debug, Clone)]
pub struct OperationMetric {
    pub name: String,
    pub start_time: Option<Instant>,
    pub duration: Option<Duration>,
    pub sub_operations: Vec<OperationMetric>,
}

impl OperationMetric {
    pub fn new(name: &str) -> Self {
        OperationMetric {
            name: name.to_string(),
            start_time: None,
            duration: None,
            sub_operations: Vec::new(),
        }
    }
    
    pub fn start(&mut self) {
        self.start_time = Some(Instant::now());
    }
    
    pub fn end(&mut self) {
        if let Some(start) = self.start_time {
            self.duration = Some(start.elapsed());
        }
    }
    
    pub fn add_sub_operation(&mut self, sub_op: OperationMetric) {
        self.sub_operations.push(sub_op);
    }
    
    pub fn format_duration(&self) -> String {
        if let Some(duration) = self.duration {
            let millis = duration.as_millis();
            if millis > 1000 {
                format!("{:.2}s", duration.as_secs_f64())
            } else {
                format!("{}ms", millis)
            }
        } else {
            "pending".to_string()
        }
    }
}

/// Performance tracker for measuring query execution metrics
#[derive(Clone)]
pub struct PerformanceTracker {
    operations: Arc<Mutex<HashMap<String, OperationMetric>>>,
    start_time: Instant,
}

impl PerformanceTracker {
    pub fn new() -> Self {
        PerformanceTracker {
            operations: Arc::new(Mutex::new(HashMap::new())),
            start_time: Instant::now(),
        }
    }
    
    pub fn start_operation(&self, name: &str) {
        if let Ok(mut ops) = self.operations.lock() {
            let mut op = OperationMetric::new(name);
            op.start();
            ops.insert(name.to_string(), op);
        }
    }
    
    pub fn end_operation(&self, name: &str) {
        if let Ok(mut ops) = self.operations.lock() {
            if let Some(op) = ops.get_mut(name) {
                op.end();
            }
        }
    }
    
    pub fn add_sub_operation(&self, parent: &str, child: &str) {
        if let Ok(mut ops) = self.operations.lock() {
            // First get a clone of the child operation
            let child_op = ops.get(child).cloned();
            
            // Then, if both exist, add the child to the parent
            if let (Some(parent_op), Some(child_op)) = (ops.get_mut(parent), child_op) {
                parent_op.add_sub_operation(child_op);
            }
        }
    }
    
    pub fn get_operation(&self, name: &str) -> Option<OperationMetric> {
        if let Ok(ops) = self.operations.lock() {
            ops.get(name).cloned()
        } else {
            None
        }
    }
    
    pub fn get_all_operations(&self) -> Vec<OperationMetric> {
        if let Ok(ops) = self.operations.lock() {
            ops.values().cloned().collect()
        } else {
            Vec::new()
        }
    }
    
    pub fn total_elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
    
    pub fn generate_report(&self) -> String {
        let mut report = String::new();
        report.push_str(&format!("Performance Report\n"));
        report.push_str(&format!("=================\n"));
        report.push_str(&format!("Total time: {:.2}s\n\n", self.total_elapsed().as_secs_f64()));
        
        if let Ok(ops) = self.operations.lock() {
            for (name, op) in ops.iter() {
                report.push_str(&format!("{}: {}\n", name, op.format_duration()));
            }
        }
        
        report
    }
}