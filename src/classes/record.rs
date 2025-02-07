use crate::helpers::{decode_sqlite_varint, parse_value, SqliteValue};

pub struct Record {
    pub header_size: u64,
    pub serial_codes: Vec<u64>,
    pub values: Vec<SqliteValue>,
}

enum RecordType {
    Table,
    Index,
    View,
    Trigger
}

impl RecordType {
    pub fn  as_str(&self) -> &str {
        match self {
            RecordType::Table => "table",
            RecordType::Index => "index",
            RecordType::View => "view",
            RecordType::Trigger => "trigger"
        }
    }
}

pub struct SchemaRecord {
    pub record: Record,
    pub record_type: RecordType,
    pub record_name: String,
    pub table_name: String,
    pub root_page: i8,
    pub sql: String
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

    // fn clone(&self) -> Record {
    //     Record {
    //         header_size: self.header_size,
    //         serial_codes: self.serial_codes.clone(),
    //         values: self.values.clone()
    //     }
    // }
}
