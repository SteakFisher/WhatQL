use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
use crate::classes::Page;
use crate::classes::page::{PageHeader, SchemaPage};
use crate::{SQLITE_HEADER_SIZE, SQLITE_PAGE_HEADER_SIZE};

pub struct Database {
    file_location: String,
    file: File
}

pub struct DatabaseHeader {
    pub page_size: u16,
    pub raw_data: Vec<u8>
}

impl Database {
    pub fn new(file_location: String) -> Database {
        let file = File::open(&file_location).unwrap().try_clone().unwrap();
        Database {
            file_location,
            file,
        }
    }

    pub fn header(&self) -> Result<DatabaseHeader, std::io::Error> {
        let mut file = &self.file;
        file.seek(SeekFrom::Start(0))?;
        let mut header = [0; SQLITE_HEADER_SIZE];
        file.read_exact(&mut header)?;

        let db_header = DatabaseHeader {
            page_size: u16::from_be_bytes([header[16], header[17]]),
            raw_data: header.to_vec()
        };

        Ok(db_header)
    }

    pub fn get_schema(&self) -> Result<SchemaPage, std::io::Error> {
        let page_size = self.header()?.page_size as u64;
        let mut file = &self.file;
        file.seek(SeekFrom::Start(0))?;
        let mut page = vec![0; page_size as usize];
        file.read_exact(&mut page)?;

        let schema_page = Page::new(page[100..page.len()].to_vec(), 1);

        let mut schema = SchemaPage {
            db_header: self.header()?,
            page: schema_page
        };

        schema.page.raw_data = page;
        Ok(schema)
    }

    pub fn get_page(&self, page_number: u32) -> Result<Page, std::io::Error> {
        let page_size = self.header()?.page_size as u64;
        let mut file = &self.file;
        file.seek(SeekFrom::Start(0))?;
        let mut page = vec![0; page_size as usize];
        file.seek(SeekFrom::Start(page_number as u64 * page_size))?;
        file.read_exact(&mut page)?;
        let data = page.clone()[(SQLITE_PAGE_HEADER_SIZE)..page.len()].to_vec();
        Ok(Page::new(data, page_number))
    }
}