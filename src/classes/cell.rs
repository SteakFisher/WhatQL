use crate::classes::Database;
use crate::classes::page::PageType;
use crate::classes::record::Record;
use crate::helpers::{decode_sqlite_varint, parse_value, SqliteValue};

pub struct Cell {
    pub record_size: u64,
    pub row_id: u64,
    pub record: Record,
    pub super_struct: CellSuper
}

pub struct CellSuper {
    pub db: Database,
    pub page: PageType,
    pub raw_data: Vec<u8>,
}

impl Cell {
    pub fn new(cell_header_offset: usize, super_struct: CellSuper) -> Result<Cell, std::io::Error> {
        let mut varint_offset = 0;

        let (record_size, record_offset) = decode_sqlite_varint(&super_struct.raw_data[cell_header_offset..cell_header_offset + 9]);
        varint_offset += record_offset;

        let (row_id, row_id_offset) = decode_sqlite_varint(&super_struct.raw_data[cell_header_offset + varint_offset..cell_header_offset + varint_offset + 9]);
        varint_offset += row_id_offset;

        let record = Record::new(super_struct.raw_data.clone(), cell_header_offset + varint_offset)?;

        Ok(Cell {
            record_size,
            row_id,
            record,
            super_struct
        })
    }
}