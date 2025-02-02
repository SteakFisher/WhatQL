use crate::classes::database::DatabaseHeader;
use crate::{SQLITE_HEADER_SIZE, SQLITE_PAGE_HEADER_SIZE};
use crate::helpers::decode_sqlite_varint;

pub struct PageHeader {
    pub page_type: u8,
    pub first_free_block: u16,
    pub num_cells: u16,
    pub start_of_cell_content_area: u16,
    pub num_frag_free_bytes: u8
}

pub struct Page {
    pub page_number: u32,
    pub raw_data: Vec<u8>,
    pub page_header: PageHeader,
    pub data: Vec<u8>
}

pub struct SchemaPage {
    pub db_header: DatabaseHeader,
    pub page: Page,
}

impl Page {
    pub fn new(raw_data: Vec<u8>, page_number: u32) -> Page {
        let page_header = PageHeader {
            page_type: raw_data[0],
            first_free_block: u16::from_be_bytes([raw_data[1], raw_data[2]]),
            num_cells: u16::from_be_bytes([raw_data[3], raw_data[4]]),
            start_of_cell_content_area: u16::from_be_bytes([raw_data[5], raw_data[6]]),
            num_frag_free_bytes: raw_data[7]
        };

        let data = raw_data[SQLITE_PAGE_HEADER_SIZE..raw_data.len()].to_vec();
        Page {
            page_number,
            raw_data,
            page_header,
            data
        }
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

    pub fn get_cell_content(&self, offset: u16) -> Vec<u8> {
        let db_header_offset = if self.page_number == 1 { SQLITE_HEADER_SIZE } else { 0 };

        let cell_header_offset =  offset as usize - db_header_offset;

        let cell_size = decode_sqlite_varint(&self.raw_data[cell_header_offset..cell_header_offset + 9]);
        println!("Size: {:?}", cell_size);

        return vec![];
    }
}