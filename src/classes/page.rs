use crate::classes::database::DatabaseHeader;
use crate::SQLITE_PAGE_HEADER_SIZE;

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
}