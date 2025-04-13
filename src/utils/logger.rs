//! Logging functionality for the WhatQL engine
//!
//! Provides structured logging with different severity levels
//! and formatting options.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Logging severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warning,
    Error,
    Fatal,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogLevel::Trace => write!(f, "TRACE"),
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warning => write!(f, "WARN"),
            LogLevel::Error => write!(f, "ERROR"),
            LogLevel::Fatal => write!(f, "FATAL"),
        }
    }
}

/// A log entry in the logger
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp: u64,
    pub level: LogLevel,
    pub message: String,
    pub component: Option<String>,
}

impl LogEntry {
    pub fn new(level: LogLevel, message: &str, component: Option<&str>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        LogEntry {
            timestamp,
            level,
            message: message.to_string(),
            component: component.map(|s| s.to_string()),
        }
    }

    pub fn format(&self) -> String {
        let level_str = match self.level {
            LogLevel::Trace => "\x1b[90m[TRACE]\x1b[0m",   // Gray
            LogLevel::Debug => "\x1b[36m[DEBUG]\x1b[0m",   // Cyan
            LogLevel::Info => "\x1b[32m[INFO]\x1b[0m",     // Green
            LogLevel::Warning => "\x1b[33m[WARN]\x1b[0m",  // Yellow
            LogLevel::Error => "\x1b[31m[ERROR]\x1b[0m",   // Red
            LogLevel::Fatal => "\x1b[1;31m[FATAL]\x1b[0m", // Bold Red
        };

        let component_str = if let Some(comp) = &self.component {
            format!("\x1b[1m[{}]\x1b[0m ", comp)
        } else {
            String::new()
        };

        format!(
            "{} {} {}{}",
            format_timestamp(self.timestamp),
            level_str,
            component_str,
            self.message
        )
    }
}

fn format_timestamp(timestamp: u64) -> String {
    // Convert UNIX timestamp to readable format
    // For simplicity, just return the timestamp as-is
    timestamp.to_string()
}

/// Core logger for the WhatQL engine
#[derive(Clone)]
pub struct Logger {
    min_level: LogLevel,
    entries: Arc<Mutex<VecDeque<LogEntry>>>,
    max_entries: usize,
}

impl Logger {
    pub fn new(min_level: LogLevel) -> Self {
        Logger {
            min_level,
            entries: Arc::new(Mutex::new(VecDeque::with_capacity(1000))),
            max_entries: 1000,
        }
    }

    pub fn log(&self, level: LogLevel, message: &str) {
        if level < self.min_level {
            return;
        }

        let entry = LogEntry::new(level, message, None);

        // Print to console
        println!("{}", entry.format());

        // Store in history
        if let Ok(mut entries) = self.entries.lock() {
            if entries.len() >= self.max_entries {
                entries.pop_front();
            }
            entries.push_back(entry);
        }
    }

    pub fn log_with_component(&self, level: LogLevel, component: &str, message: &str) {
        if level < self.min_level {
            return;
        }

        let entry = LogEntry::new(level, message, Some(component));

        // Print to console
        println!("{}", entry.format());

        // Store in history
        if let Ok(mut entries) = self.entries.lock() {
            if entries.len() >= self.max_entries {
                entries.pop_front();
            }
            entries.push_back(entry);
        }
    }

    pub fn get_entries(&self) -> Vec<LogEntry> {
        if let Ok(entries) = self.entries.lock() {
            entries.iter().cloned().collect()
        } else {
            Vec::new()
        }
    }

    pub fn get_entries_by_level(&self, level: LogLevel) -> Vec<LogEntry> {
        if let Ok(entries) = self.entries.lock() {
            entries
                .iter()
                .filter(|e| e.level == level)
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn clear(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.clear();
        }
    }
}
