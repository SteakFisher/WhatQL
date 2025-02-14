use std::io::Error;
use crate::classes::database::DatabaseHeader;
use crate::classes::{Cell, Database, RecordType};
use crate::{SQLITE_HEADER_SIZE, SQLITE_PAGE_HEADER_SIZE};
use crate::classes::cell::CellSuper;
use crate::classes::record::SchemaRecord;
use crate::helpers::SqliteValue;

pub struct PageHeader {
    pub page_type: u8,
    pub first_free_block: u16,
    pub num_cells: u16,
    pub start_of_cell_content_area: u16,
    pub num_frag_free_bytes: u8
}

pub enum PageType {
    Schema(SchemaPage),
    Data(DataPage),
}

pub struct PageSuper {
    pub db: Database,
    pub raw_data: Vec<u8>
}

impl PageSuper {
    pub fn clone(&self) -> PageSuper {
        PageSuper {
            db: self.db.clone(),
            raw_data: self.raw_data.clone()
        }
    }
}

impl PageType {
    pub fn clone(&self) -> PageType {
        match self {
            PageType::Schema(schema) => PageType::Schema(schema.clone()),
            PageType::Data(data) => PageType::Data(data.clone())
        }
    }
}

pub struct DataPage {
    pub page_number: u32,
    pub page_header: PageHeader,
    pub data: Vec<u8>,
    pub super_struct: PageSuper
}

pub struct SchemaPage {
    pub db_header: DatabaseHeader,
    pub page_number: u32,
    pub page_header: PageHeader,
    pub data: Vec<u8>,
    pub super_struct: PageSuper
}

impl PageHeader {
    fn new(raw_data: Vec<u8>) -> PageHeader {
        PageHeader {
            page_type: raw_data[0],
            first_free_block: u16::from_be_bytes([raw_data[1], raw_data[2]]),
            num_cells: u16::from_be_bytes([raw_data[3], raw_data[4]]),
            start_of_cell_content_area: u16::from_be_bytes([raw_data[5], raw_data[6]]),
            num_frag_free_bytes: raw_data[7]
        }
    }

    fn clone(&self) -> PageHeader {
        PageHeader {
            page_type: self.page_type,
            first_free_block: self.first_free_block,
            num_cells: self.num_cells,
            start_of_cell_content_area: self.start_of_cell_content_area,
            num_frag_free_bytes: self.num_frag_free_bytes
        }
    }
}

impl  SchemaPage {
    pub fn new(super_struct: PageSuper) -> SchemaPage {
        let page_header = PageHeader::new(super_struct.raw_data[SQLITE_HEADER_SIZE..100 + SQLITE_PAGE_HEADER_SIZE].to_vec());

        let data = super_struct.raw_data[SQLITE_HEADER_SIZE..].to_vec();

        let header = super_struct.db.header().unwrap();

        SchemaPage {
            db_header: header,
            page_number: 1,
            page_header,
            data: data[SQLITE_PAGE_HEADER_SIZE..].to_vec(),
            super_struct,
        }
    }

    pub fn get_cell_offsets(&self) -> Vec<u16> {
        let mut offsets: Vec<u16> = Vec::with_capacity(self.page_header.num_cells as usize);

        for i in 0..self.page_header.num_cells {
            let offset_index = SQLITE_HEADER_SIZE + SQLITE_PAGE_HEADER_SIZE + (i * 2) as usize;
            let offset = u16::from_be_bytes([
                self.super_struct.raw_data[offset_index],
                self.super_struct.raw_data[offset_index + 1]
            ]);
            offsets.push(offset);
        }

        offsets
    }

    pub fn get_cell_contents(&self) -> Vec<Cell> {
        let mut cells: Vec<Cell> = Vec::with_capacity(self.page_header.num_cells as usize);

        for i in 0..self.page_header.num_cells {
            let offset_index = SQLITE_HEADER_SIZE + SQLITE_PAGE_HEADER_SIZE + (i * 2) as usize;
            let offset = u16::from_be_bytes([
                self.super_struct.raw_data[offset_index],
                self.super_struct.raw_data[offset_index + 1]
            ]);
            let cell = self.get_cell_content(offset).unwrap();
            cells.push(cell);
        }

        cells
    }

    pub fn get_cell_content(&self, offset: u16) -> Result<Cell, Error> {
        let cell_data = self.super_struct.raw_data.to_vec();
        Cell::new(offset as usize, CellSuper {
            db: self.super_struct.db.clone(),
            page: PageType::Schema(self.clone()),
            raw_data: cell_data
        })
    }

    pub fn get_table_data(&self) -> Vec<SchemaRecord> {
        let mut table_names: Vec<SchemaRecord> = Vec::new();

        for cell in self.get_cell_contents() {
            match cell.record {
                RecordType::SchemaRecord(record) => {
                    table_names.push(record.clone());
                },
                _ => {}
            }
        }

        table_names
    }

    pub fn clone(&self) -> SchemaPage {
        SchemaPage {
            db_header: self.db_header.clone(),
            page_number: self.page_number,
            page_header: self.page_header.clone(),
            data: self.data.clone(),
            super_struct: self.super_struct.clone()
        }
    }
}

impl DataPage {
    pub fn new(page_number: u32, super_struct: PageSuper) -> DataPage {
        let page_header = PageHeader::new(super_struct.raw_data[..SQLITE_PAGE_HEADER_SIZE].to_vec());

        let data = super_struct.raw_data[SQLITE_PAGE_HEADER_SIZE..super_struct.raw_data.len()].to_vec();
        DataPage {
            page_number,
            page_header,
            data,
            super_struct
        }
    }

    pub fn get_columns(&self) -> Vec<SchemaRecord> {
        let mut columns: Vec<SchemaRecord> = Vec::new();

        let schema = self.super_struct.db.get_schema().unwrap().get_table_data();

        let mut sql_create_queries: Vec<String> = Vec::new();

        for i in schema {
            if let SqliteValue::Text(sql_create_query) = &i.values[4] {
                sql_create_queries.push(sql_create_query.to_string())
            }
        }

        for query in sql_create_queries {

        }

        columns
    }

    pub fn get_cell_offsets(&self) -> Vec<u16> {
        let mut offsets: Vec<u16> = Vec::with_capacity(self.page_header.num_cells as usize);

        for i in 0..self.page_header.num_cells {
            let offset_index = SQLITE_PAGE_HEADER_SIZE + (i * 2) as usize;
            let offset = u16::from_be_bytes([
                self.super_struct.raw_data[offset_index],
                self.super_struct.raw_data[offset_index + 1]
            ]);
            offsets.push(offset);
        }

        offsets
    }

    pub fn get_cell_contents(&self) -> Vec<Cell> {
        let mut cells: Vec<Cell> = Vec::with_capacity(self.page_header.num_cells as usize);

        for i in 0..self.page_header.num_cells {
            let offset_index = SQLITE_PAGE_HEADER_SIZE + (i * 2) as usize;
            let offset = u16::from_be_bytes([
                self.super_struct.raw_data[offset_index],
                self.super_struct.raw_data[offset_index + 1]
            ]);
            let cell = self.get_cell_content(offset).unwrap();
            cells.push(cell);
        }

        cells
    }

    pub fn get_cell_content(&self, offset: u16) -> Result<Cell, std::io::Error> {
        let db_header_offset = if self.page_number == 1 { SQLITE_HEADER_SIZE } else { 0 };

        let cell_header_offset =  offset as usize - db_header_offset;

        let cell_data = self.super_struct.raw_data.to_vec();

        Cell::new(cell_header_offset, CellSuper {
            db: self.super_struct.db.clone(),
            page: PageType::Data(self.clone()),
            raw_data: cell_data
        })
    }

    pub fn clone(&self) -> DataPage {
        DataPage {
            page_number: self.page_number,
            page_header: self.page_header.clone(),
            data: self.data.clone(),
            super_struct: self.super_struct.clone()
        }
    }
}