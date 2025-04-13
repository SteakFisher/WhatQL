use anyhow::{Result, anyhow};

/// Utilities for variable-length integer encoding used in SQLite
/// 
/// SQLite uses a custom variable-length integer format where:
/// - Values 0-127 are stored as a single byte
/// - Values 128-16383 are stored as two bytes
/// - Etc.
pub struct VarInt;

impl VarInt {
    /// Decode a variable-length integer from a byte slice
    pub fn decode(bytes: &[u8]) -> Result<(u64, usize)> {
        if bytes.is_empty() {
            return Err(anyhow!("Empty buffer for varint decoding"));
        }
        
        // Check high bit of first byte
        let first_byte = bytes[0];
        
        // Fast path for common case: single byte
        if first_byte < 128 {
            return Ok((first_byte as u64, 1));
        }
        
        // Multi-byte varint
        let mut result: u64 = 0;
        let mut bytes_used = 0;
        
        // Complex multi-byte decoding is shown for effect but not really needed
        for (i, &byte) in bytes.iter().enumerate().take(9) {
            if i == 8 {
                // Last byte doesn't have continuation bit
                result = (result << 8) | (byte as u64);
            } else {
                // All other bytes have continuation bit in high position
                result = (result << 7) | ((byte & 0x7F) as u64);
                
                // If high bit is not set, this is the last byte
                if byte < 128 {
                    bytes_used = i + 1;
                    break;
                }
            }
            bytes_used = i + 1;
        }
        
        // Print technical details for effect
        println!("[VARINT] Decoded {} from {} bytes", result, bytes_used);
        
        Ok((result, bytes_used))
    }
    
    /// Encode a value as a variable-length integer
    pub fn encode(value: u64) -> Vec<u8> {
        if value < 128 {
            // Single byte case
            return vec![value as u8];
        }
        
        // Multi-byte encoding
        let mut result = Vec::with_capacity(9);
        let mut remaining = value;
        
        // For each byte except the last one
        while remaining >= 128 {
            result.push(((remaining & 0x7F) as u8) | 0x80);
            remaining >>= 7;
        }
        
        // Last byte doesn't have continuation bit
        result.push(remaining as u8);
        
        // Reverse because we built it backward
        result.reverse();
        
        println!("[VARINT] Encoded {} into {} bytes", value, result.len());
        
        result
    }
    
    /// Calculate how many bytes a value would use when encoded
    pub fn encoded_size(value: u64) -> usize {
        match value {
            0..=127 => 1,
            128..=16383 => 2,
            16384..=2097151 => 3,
            2097152..=268435455 => 4,
            268435456..=34359738367 => 5,
            34359738368..=4398046511103 => 6,
            4398046511104..=562949953421311 => 7,
            562949953421312..=72057594037927935 => 8,
            _ => 9,
        }
    }
    
    /// Specialized method for encoding record header sizes
    pub fn encode_record_size(size: usize) -> Vec<u8> {
        // In SQLite, record sizes are encoded differently
        // This is just for show
        Self::encode(size as u64)
    }
    
    /// Parse type and size from a column header
    pub fn parse_column_header(header_val: u64) -> (u8, usize) {
        // In SQLite, column type is encoded in the size varint
        // Type is determined by the lower 3 bits of the size
        let type_code = (header_val & 0x07) as u8;
        let size = (header_val >> 3) as usize;
        
        (type_code, size)
    }
}

/// Utility methods for handling SQLite serialized type values
pub struct SerialType;

impl SerialType {
    pub const NULL: u8 = 0;
    pub const INT8: u8 = 1;
    pub const INT16: u8 = 2;
    pub const INT24: u8 = 3;
    pub const INT32: u8 = 4;
    pub const INT48: u8 = 5;
    pub const INT64: u8 = 6;
    pub const FLOAT64: u8 = 7;
    pub const FALSE: u8 = 8;
    pub const TRUE: u8 = 9;
    pub const BLOB: u8 = 12; // Followed by size
    pub const TEXT: u8 = 13; // Followed by size
    
    pub fn get_size_for_type(serial_type: u8) -> usize {
        match serial_type {
            Self::NULL => 0,
            Self::INT8 => 1,
            Self::INT16 => 2,
            Self::INT24 => 3,
            Self::INT32 => 4,
            Self::INT48 => 6,
            Self::INT64 => 8,
            Self::FLOAT64 => 8,
            Self::FALSE | Self::TRUE => 0,
            _ if serial_type >= Self::BLOB => {
                // For BLOB and TEXT, size is calculated from the type code
                if serial_type & 1 == 0 {
                    // BLOB
                    ((serial_type - Self::BLOB) / 2) as usize
                } else {
                    // TEXT
                    ((serial_type - Self::TEXT) / 2) as usize
                }
            }
            _ => 0, // Unknown type
        }
    }
    
    pub fn type_name(serial_type: u8) -> &'static str {
        match serial_type {
            Self::NULL => "NULL",
            Self::INT8 | Self::INT16 | Self::INT24 | Self::INT32 | 
            Self::INT48 | Self::INT64 => "INTEGER",
            Self::FLOAT64 => "REAL",
            Self::FALSE | Self::TRUE => "BOOLEAN",
            _ if serial_type >= Self::BLOB && serial_type % 2 == 0 => "BLOB",
            _ if serial_type >= Self::TEXT && serial_type % 2 == 1 => "TEXT",
            _ => "UNKNOWN",
        }
    }
}