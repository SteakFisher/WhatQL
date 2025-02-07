use crate::classes::record::Record;
use crate::helpers::{decode_sqlite_varint, parse_value, SqliteValue};

pub struct Cell {
    pub record_size: u64,
    pub row_id: u64,
    pub record: Record,
    pub raw_data: Vec<u8>,
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