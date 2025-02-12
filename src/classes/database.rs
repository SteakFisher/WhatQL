use crate::classes::page::{PageSuper, PageType, SchemaPage};
use crate::SQLITE_HEADER_SIZE;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
use crate::classes::DataPage;

pub struct Database {
    file_location: String,
    file: File
}

pub struct DatabaseHeader {
    pub header_string: u128,
    pub page_size: u16,
    pub write_format_version: u8,
    pub read_format_version: u8,
    pub reserved_space: u8,
    pub max_embedded_payload_fraction: u8,
    pub min_embedded_payload_fraction: u8,
    pub leaf_payload_fraction: u8,
    pub file_change_counter: u32,
    pub database_size: u32,
    pub first_freelist_trunk_page: u32,
    pub total_freelist_pages: u32,
    pub schema_cookie: u32,
    pub schema_format: u32,
    pub default_page_cache_size: u32,
    pub largest_root_b_tree_page_number: u32,
    pub text_encoding: u32,
    pub user_version: u32,
    pub incremental_vacuum_mode: u32,
    pub application_id: u32,
    pub version_valid_for: u32,
    pub sqlite_version_number: u32,
    pub raw_data: Vec<u8>
}

impl DatabaseHeader {
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.raw_data).unwrap()
    }

    pub fn clone(&self) -> DatabaseHeader {
        DatabaseHeader {
            header_string: self.header_string,
            page_size: self.page_size,
            write_format_version: self.write_format_version,
            read_format_version: self.read_format_version,
            reserved_space: self.reserved_space,
            max_embedded_payload_fraction: self.max_embedded_payload_fraction,
            min_embedded_payload_fraction: self.min_embedded_payload_fraction,
            leaf_payload_fraction: self.leaf_payload_fraction,
            file_change_counter: self.file_change_counter,
            database_size: self.database_size,
            first_freelist_trunk_page: self.first_freelist_trunk_page,
            total_freelist_pages: self.total_freelist_pages,
            schema_cookie: self.schema_cookie,
            schema_format: self.schema_format,
            default_page_cache_size: self.default_page_cache_size,
            largest_root_b_tree_page_number: self.largest_root_b_tree_page_number,
            text_encoding: self.text_encoding,
            user_version: self.user_version,
            incremental_vacuum_mode: self.incremental_vacuum_mode,
            application_id: self.application_id,
            version_valid_for: self.version_valid_for,
            sqlite_version_number: self.sqlite_version_number,
            raw_data: self.raw_data.clone()
        }
    }
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
            write_format_version: header[18],
            read_format_version: header[19],
            reserved_space: header[20],
            max_embedded_payload_fraction: header[21],
            min_embedded_payload_fraction: header[22],
            leaf_payload_fraction: header[23],
            file_change_counter: u32::from_be_bytes([header[24], header[25], header[26], header[27]]),
            database_size: u32::from_be_bytes([header[28], header[29], header[30], header[31]]),
            first_freelist_trunk_page: u32::from_be_bytes([header[32], header[33], header[34], header[35]]),
            total_freelist_pages: u32::from_be_bytes([header[36], header[37], header[38], header[39]]),
            schema_cookie: u32::from_be_bytes([header[40], header[41], header[42], header[43]]),
            schema_format: u32::from_be_bytes([header[44], header[45], header[46], header[47]]),
            default_page_cache_size: u32::from_be_bytes([header[48], header[49], header[50], header[51]]),
            largest_root_b_tree_page_number: u32::from_be_bytes([header[52], header[53], header[54], header[55]]),
            text_encoding: u32::from_be_bytes([header[56], header[57], header[58], header[59]]),
            user_version: u32::from_be_bytes([header[60], header[61], header[62], header[63]]),
            incremental_vacuum_mode: u32::from_be_bytes([header[64], header[65], header[66], header[67]]),
            application_id: u32::from_be_bytes([header[68], header[69], header[70], header[71]]),
            version_valid_for: u32::from_be_bytes([header[72], header[73], header[74], header[75]]),
            sqlite_version_number: u32::from_be_bytes([header[76], header[77], header[78], header[79]]),
            header_string: u128::from_be_bytes([
                header[0], header[1], header[2], header[3], header[4], header[5], header[6], header[7],
                header[8], header[9], header[10], header[11], header[12], header[13], header[14], header[15]
            ]),
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

        let super_struct = PageSuper {
            db: self.clone(),
            raw_data: page
        };

        let schema = SchemaPage::new(super_struct);

        Ok(schema)
    }

    pub fn get_page(&self, page_number: u32) -> Result<DataPage, std::io::Error> {
        let page_size = self.header()?.page_size as u64;
        let mut file = &self.file;
        file.seek(SeekFrom::Start(0))?;
        let mut page = vec![0; page_size as usize];
        file.seek(SeekFrom::Start(page_number as u64 * page_size))?;
        file.read_exact(&mut page)?;
        // println!("page: {:?}", page);
        Ok(DataPage::new(page_number, PageSuper {
            db: self.clone(),
            raw_data: page
        }))
    }

    pub fn clone(&self) -> Database {
        Database {
            file_location: self.file_location.clone(),
            file: self.file.try_clone().unwrap()
        }
    }
}