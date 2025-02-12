use crate::classes::Database;
use crate::classes::page::{PageSuper, PageType};
use crate::classes::record::{DataRecord, RecordSuper, RecordType, SchemaColumns, SchemaRecord, SchemaRecordType};
use crate::helpers::{decode_sqlite_varint, parse_value, SqliteValue};

pub struct Cell {
    pub record_size: u64,
    pub row_id: u64,
    pub record: RecordType,
    pub super_struct: CellSuper
}

pub struct CellSuper {
    pub db: Database,
    pub page: PageType,
    pub raw_data: Vec<u8>,
}

impl CellSuper {
    pub fn clone(&self) -> CellSuper {
        CellSuper {
            db: self.db.clone(),
            page: self.page.clone(),
            raw_data: self.raw_data.clone()
        }
    }
}

impl Cell {
    pub fn new(cell_header_offset: usize, super_struct: CellSuper) -> Result<Cell, std::io::Error> {
        let mut varint_offset = 0;

        let (record_size, record_offset) = decode_sqlite_varint(&super_struct.raw_data[cell_header_offset..cell_header_offset + 9]);
        varint_offset += record_offset;

        let (row_id, row_id_offset) = decode_sqlite_varint(&super_struct.raw_data[cell_header_offset + varint_offset..cell_header_offset + varint_offset + 9]);
        varint_offset += row_id_offset;

        let mut cell = Cell {
            record_size: 0,
            row_id: 0,
            record: RecordType::SchemaRecord(SchemaRecord {
                header_size: 0,
                serial_codes: vec![],
                values: vec![],
                columns: SchemaColumns {
                    record_type: SchemaRecordType::Table,
                    record_name: "".to_string(),
                    table_name: "".to_string(),
                    root_page: 0,
                    sql: "".to_string()
                }
            }),
            super_struct: super_struct.clone()
        };

        let super_struct_record = RecordSuper {
            db: super_struct.db.clone(),
            page: super_struct.page.clone(),
            cell: cell.clone(),
            raw_data: super_struct.raw_data.clone()
        };

        match (super_struct.page.clone()) {
            PageType::Data(page) => {
                let cell_data = page.super_struct.raw_data.to_vec();
                let record = DataRecord::new(cell_header_offset + varint_offset, super_struct_record)?;
                cell.record = RecordType::DataRecord(record);
                Ok(cell)
            },
            PageType::Schema(schema) => {
                let cell_data = schema.super_struct.raw_data.to_vec();
                let record = SchemaRecord::new(cell_header_offset + varint_offset, super_struct_record)?;
                cell.record = RecordType::SchemaRecord(record);
                Ok(cell)
            },
        }
    }

    pub fn clone(&self) -> Cell {
        Cell {
            record_size: self.record_size,
            row_id: self.row_id,
            record: self.record.clone(),
            super_struct: self.super_struct.clone()
        }
    }
}