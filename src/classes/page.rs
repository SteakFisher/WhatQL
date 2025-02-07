use crate::classes::database::DatabaseHeader;
use crate::classes::{Cell, Database};
use crate::{SQLITE_HEADER_SIZE, SQLITE_PAGE_HEADER_SIZE};

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

pub enum PageSuper {
    Database(Database)
}

impl PageSuper {
    pub fn clone(&self) -> PageSuper {
        match self {
            PageSuper::Database(db) => PageSuper::Database(db.clone())
        }
    }
}

pub struct DataPage {
    pub page_number: u32,
    pub raw_data: Vec<u8>,
    pub page_header: PageHeader,
    pub data: Vec<u8>,
    pub super_struct: PageSuper
}

pub struct SchemaPage {
    pub db_header: DatabaseHeader,
    pub page: DataPage,
    pub super_struct: PageSuper,
    raw_data: Vec<u8>
}

impl SchemaPage {
    pub fn new(raw_data: Vec<u8>, super_struct: PageSuper) -> SchemaPage {
        let page_header = PageHeader::new(raw_data[SQLITE_HEADER_SIZE..100 + SQLITE_PAGE_HEADER_SIZE].to_vec());

        let data = raw_data[SQLITE_HEADER_SIZE..].to_vec();

        let header = match super_struct.clone() {
            PageSuper::Database(schema_page) => {
                schema_page.header().unwrap()
            },
        };

        SchemaPage {
            db_header: header,
            page: DataPage {
                page_number: 1,
                raw_data: data.clone(),
                page_header,
                data: data[SQLITE_PAGE_HEADER_SIZE..].to_vec(),
                super_struct: super_struct.clone()
            },
            super_struct: super_struct.clone(),
            raw_data
        }
    }

    pub fn get_cell_offsets(&self) -> Vec<u16> {
        let mut offsets: Vec<u16> = Vec::with_capacity(self.page.page_header.num_cells as usize);

        for i in 0..self.page.page_header.num_cells {
            let offset_index = SQLITE_PAGE_HEADER_SIZE + (i * 2) as usize;
            let offset = u16::from_be_bytes([
                self.page.raw_data[offset_index],
                self.page.raw_data[offset_index + 1]
            ]);
            offsets.push(offset);
        }

        offsets
    }
}

impl PageHeader {
    pub fn new(raw_data: Vec<u8>) -> PageHeader {
        PageHeader {
            page_type: raw_data[0],
            first_free_block: u16::from_be_bytes([raw_data[1], raw_data[2]]),
            num_cells: u16::from_be_bytes([raw_data[3], raw_data[4]]),
            start_of_cell_content_area: u16::from_be_bytes([raw_data[5], raw_data[6]]),
            num_frag_free_bytes: raw_data[7]
        }
    }
}

impl DataPage {
    pub fn new(raw_data: Vec<u8>, page_number: u32, super_struct: PageSuper) -> PageType {
        let page_header = PageHeader::new(raw_data[..SQLITE_PAGE_HEADER_SIZE].to_vec());


        let data = raw_data[SQLITE_PAGE_HEADER_SIZE..raw_data.len()].to_vec();
        PageType::Data(DataPage {
            page_number,
            raw_data,
            page_header,
            data,
            super_struct
        })
    }

    pub fn get_cell_offsets(&self) -> Vec<u16> {
        let mut offsets: Vec<u16> = Vec::with_capacity(self.page_header.num_cells as usize);

        for i in 0..self.page_header.num_cells {
            let offset_index = SQLITE_PAGE_HEADER_SIZE + (i * 2) as usize;
            let offset = u16::from_be_bytes([
                self.raw_data[offset_index],
                self.raw_data[offset_index + 1]
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
                self.raw_data[offset_index],
                self.raw_data[offset_index + 1]
            ]);
            let cell = self.get_cell_content(offset).unwrap();
            cells.push(cell);
        }

        cells
    }

    pub fn get_cell_content(&self, offset: u16) -> Result<Cell, std::io::Error> {
        let db_header_offset = if self.page_number == 1 { SQLITE_HEADER_SIZE } else { 0 };

        let cell_header_offset =  offset as usize - db_header_offset;

        let cell_data = self.raw_data.to_vec();

        Cell::new(cell_data, cell_header_offset)
    }
}