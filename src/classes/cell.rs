use std::collections::HashMap;
use crate::helpers::{decode_sqlite_varint, parse_value, SqliteValue};

pub struct Record {
    pub header_size: u64,
    pub serial_codes: Vec<u64>,
    pub values: Vec<SqliteValue>,
}

pub struct Cell {
    pub record_size: u64,
    pub row_id: u64,
    pub record: Record,
    pub raw_data: Vec<u8>,
}


impl Record {
    pub fn new(raw_data: Vec<u8>, mut offset: usize) -> Result<Record, std::io::Error> {
        let (header_size, header_size_offset) = decode_sqlite_varint(&raw_data[offset..offset + 9]);
        offset += header_size_offset;

        let mut serial_codes = vec![];

        let mut index = 0;
        while index < header_size - 1 {
            let (size, size_bytes) = decode_sqlite_varint(&raw_data[offset + (index as usize)..offset + (index as usize) + 9]);
            serial_codes.push(size);
            index += size_bytes as u64;
        }
        offset += header_size as usize - 1;

        let mut values = vec![];

        for serial_code in serial_codes.clone() {
            let parsed_result = parse_value(serial_code, &raw_data[offset..])?;
            values.push(parsed_result.value);
            offset += parsed_result.bytes_consumed;
        }

        Ok(Record {
            header_size,
            serial_codes,
            values,
        })
    }
}

impl Cell {
    pub fn new(raw_data: Vec<u8>, cell_header_offset: usize) -> Result<Cell, std::io::Error> {
        let mut varint_offset = 0;

        let (record_size, record_offset) = decode_sqlite_varint(&raw_data[cell_header_offset..cell_header_offset + 9]);
        varint_offset += record_offset;

        let (row_id, row_id_offset) = decode_sqlite_varint(&raw_data[cell_header_offset + varint_offset..cell_header_offset + varint_offset + 9]);
        varint_offset += row_id_offset;

        let record = Record::new(raw_data.clone(), cell_header_offset + varint_offset)?;

        Ok(Cell {
            record_size,
            row_id,
            record,
            raw_data
        })
    }
}