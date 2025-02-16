use crate::classes::{Cell, Database};
use crate::classes::page::PageType;
use crate::helpers::{decode_sqlite_varint, parse_value, SqliteValue};

pub struct DataRecord {
    pub header_size: u64,
    pub serial_codes: Vec<u64>,
    pub values: Vec<SqliteValue>,
}

pub enum RecordType {
    SchemaRecord(SchemaRecord),
    DataRecord(DataRecord)
}

impl RecordType {
    pub fn clone(&self) -> RecordType {
        match self {
            RecordType::SchemaRecord(schema) => RecordType::SchemaRecord(schema.clone()),
            RecordType::DataRecord(data) => RecordType::DataRecord(data.clone())
        }
    }
}

#[derive(Debug)]
pub enum SchemaRecordType {
    Table,
    Index,
    View,
    Trigger
}

impl SchemaRecordType {
    pub fn  as_str(&self) -> &str {
        match self {
            SchemaRecordType::Table => "table",
            SchemaRecordType::Index => "index",
            SchemaRecordType::View => "view",
            SchemaRecordType::Trigger => "trigger"
        }
    }
}

#[derive(Debug)]
pub struct SchemaColumns {
    pub record_type: SchemaRecordType,
    pub record_name: String,
    pub table_name: String,
    pub root_page: i8,
    pub sql: String
}

#[derive(Debug)]
pub struct SchemaRecord {
    pub header_size: u64,
    pub serial_codes: Vec<u64>,
    pub values: Vec<SqliteValue>,
    pub columns: SchemaColumns
}

pub struct RecordSuper {
    pub db: Database,
    pub page: PageType,
    pub cell: Cell,
    pub raw_data: Vec<u8>,
}

impl SchemaColumns {
    pub fn clone(&self) -> SchemaColumns {
        SchemaColumns {
            record_type: self.record_type.clone(),
            record_name: self.record_name.clone(),
            table_name: self.table_name.clone(),
            root_page: self.root_page,
            sql: self.sql.clone()
        }
    }
}

impl SchemaRecordType {
    pub fn clone(&self) -> SchemaRecordType {
        match self {
            SchemaRecordType::Table => SchemaRecordType::Table,
            SchemaRecordType::Index => SchemaRecordType::Index,
            SchemaRecordType::View => SchemaRecordType::View,
            SchemaRecordType::Trigger => SchemaRecordType::Trigger
        }
    }
}

impl SchemaRecord {
    pub fn new(mut offset: usize, super_struct: RecordSuper) -> Result<SchemaRecord, std::io::Error> {
        let (header_size, header_size_offset) = decode_sqlite_varint(&super_struct.raw_data[offset..offset + 9]);
        offset += header_size_offset;

        let mut serial_codes = vec![];

        let mut index = 0;
        while index < header_size - 1 {
            let (size, size_bytes) = decode_sqlite_varint(&super_struct.raw_data[offset + (index as usize)..offset + (index as usize) + 9]);
            serial_codes.push(size);
            index += size_bytes as u64;
        }
        offset += header_size as usize - 1;

        let mut values = vec![];

        for serial_code in serial_codes.clone() {
            let parsed_result = parse_value(serial_code, &super_struct.raw_data[offset..])?;
            values.push(parsed_result.value);
            offset += parsed_result.bytes_consumed;
        }

        // todo: Implement parsing the columns properly
        let schema_cols: SchemaColumns = SchemaColumns {
            record_type: SchemaRecordType::Table,
            record_name: "".to_string(),
            table_name: "".to_string(),
            root_page: 0,
            sql: "".to_string(),
        };

        Ok(SchemaRecord {
            header_size,
            serial_codes,
            values,
            columns: schema_cols
        })
    }

    pub fn clone(&self) -> SchemaRecord {
        SchemaRecord {
            header_size: self.header_size,
            serial_codes: self.serial_codes.clone(),
            values: self.values.clone(),
            columns: self.columns.clone()
        }
    }
}

impl DataRecord {
    pub fn new(mut offset: usize, super_struct: RecordSuper) -> Result<DataRecord, std::io::Error> {
        let (header_size, header_size_offset) = decode_sqlite_varint(&super_struct.raw_data[offset..offset + 9]);
        offset += header_size_offset;

        let mut serial_codes = vec![];

        let mut index = 0;
        while index < header_size - 1 {
            let (size, size_bytes) = decode_sqlite_varint(&super_struct.raw_data[offset + (index as usize)..offset + (index as usize) + 9]);
            serial_codes.push(size);
            index += size_bytes as u64;
        }
        offset += header_size as usize - 1;

        let mut values = vec![];

        for serial_code in serial_codes.clone() {
            let parsed_result = parse_value(serial_code, &super_struct.raw_data[offset..])?;
            values.push(parsed_result.value);
            offset += parsed_result.bytes_consumed;
        }

        Ok(DataRecord {
            header_size,
            serial_codes,
            values,
        })
    }

    // pub fn parse_into_schema(&self) -> Result<SchemaRecord, std::io::Error> {
    //     let record_type = match &self.values[0] {
    //         SqliteValue::Text(text) => {
    //             match text.as_str() {
    //                 "table" => RecordType::Table,
    //                 "index" => RecordType::Index,
    //                 "view" => RecordType::View,
    //                 "trigger" => RecordType::Trigger,
    //                 _ => panic!("Invalid record type")
    //             }
    //         }
    //         _ => panic!("Invalid record type")
    //     };
    //
    //     let record_name = match &self.values[1] {
    //         SqliteValue::Text(text) => text.clone(),
    //         _ => panic!("Invalid record name")
    //     };
    //
    //     let table_name = match &self.values[2] {
    //         SqliteValue::Text(text) => text.clone(),
    //         _ => panic!("Invalid table name")
    //     };
    //
    //     let root_page = match &self.values[3] {
    //         SqliteValue::Integer(i) => *i as i8,
    //         _ => panic!("Invalid root page")
    //     };
    //
    //     let sql = match &self.values[4] {
    //         SqliteValue::Text(text) => text.clone(),
    //         _ => panic!("Invalid sql")
    //     };
    //
    //     Ok(SchemaRecord {
    //         record: (*self).clone(),
    //         record_type,
    //         record_name,
    //         table_name,
    //         root_page,
    //         sql
    //     })
    // }

    fn clone(&self) -> DataRecord {
        DataRecord {
            header_size: self.header_size,
            serial_codes: self.serial_codes.clone(),
            values: self.values.clone()
        }
    }
}
