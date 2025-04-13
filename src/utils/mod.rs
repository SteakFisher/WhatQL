//! Utility functions and shared components
//!
//! This module contains logging, metrics, and other support functionality
//! used throughout the WhatQL engine.

pub mod logger;
pub mod metrices;
pub mod sqlite_parocessor;

/// Common configuration parameters for the engine
pub struct EngineConfig {
    pub page_cache_size: usize,
    pub max_memory_usage: usize,
    pub log_level: logger::LogLevel,
    pub profiling_enabled: bool,
    pub parallel_execution: bool,
    pub debug_assertions: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            page_cache_size: 2000,
            max_memory_usage: 100 * 1024 * 1024, // 100 MB
            log_level: logger::LogLevel::Info,
            profiling_enabled: false,
            parallel_execution: true,
            debug_assertions: cfg!(debug_assertions),
        }
    }
}

/// Error handling functions
pub mod error {
    use std::fmt;
    
    #[derive(Debug, Clone)]
    pub enum ErrorLevel {
        Warning,
        Error,
        Fatal,
    }
    
    #[derive(Debug, Clone)]
    pub struct EngineError {
        pub level: ErrorLevel,
        pub message: String,
        pub code: u32,
        pub source_location: Option<String>,
    }
    
    impl fmt::Display for EngineError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "[{:?}] {}", self.level, self.message)
        }
    }
    
    pub fn format_error_chain(error: &anyhow::Error) -> String {
        let mut result = String::new();
        
        result.push_str(&format!("Error: {}\n", error));
        
        let mut source = error.source();
        let mut indent = 1;
        
        while let Some(err) = source {
            result.push_str(&format!("{:indent$}Caused by: {}\n", "", err, indent = indent * 2));
            source = err.source();
            indent += 1;
        }
        
        result
    }
}

/// Utility functions for binary data manipulation
pub mod binary {
    /// Convert a byte array to a hexadecimal string
    pub fn to_hex_string(bytes: &[u8]) -> String {
        let mut result = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            result.push_str(&format!("{:02x}", byte));
        }
        result
    }
    
    /// Convert a hexadecimal string to a byte array
    pub fn from_hex_string(hex: &str) -> Result<Vec<u8>, std::num::ParseIntError> {
        let mut bytes = Vec::with_capacity(hex.len() / 2);
        
        for i in (0..hex.len()).step_by(2) {
            if i + 2 <= hex.len() {
                let byte = u8::from_str_radix(&hex[i..i+2], 16)?;
                bytes.push(byte);
            }
        }
        
        Ok(bytes)
    }
    
    /// Convert between little and big endian
    pub fn swap_endian_u32(value: u32) -> u32 {
        ((value & 0xFF) << 24) | 
        ((value & 0xFF00) << 8) | 
        ((value & 0xFF0000) >> 8) | 
        ((value & 0xFF000000) >> 24)
    }
}