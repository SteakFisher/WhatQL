mod helpers;
mod classes;

use classes::Database;
use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
use crate::helpers::decode_sqlite_varint;

const SQLITE_HEADER_SIZE: usize = 100;
const SQLITE_PAGE_HEADER_SIZE: usize = 8;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let db = Database::new(args[1].clone());

            let header = db.header()?;

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            let page_size = header.page_size;

            // You can use print statements as follows for debugging, they'll be visible when running tests.
            eprintln!("Logs from your program will appear here!");

            // Uncomment this block to pass the first stage
            println!("database page size: {}", page_size);

            let page1 = db.get_schema()?;
            page1.db_header.page_size;

            let num_pages = page1.page.page_header.num_cells;

            println!("number of tables: {}", num_pages);
        }
        ".tables" => {
            let db = Database::new(args[1].clone());

            let schema = db.get_schema()?;
            let page1 = schema.page.raw_data;

            eprintln!("Page type: {}", schema.page.page_header.page_type);

            let num_cells =schema.page.page_header.num_cells;
            eprintln!("Type of B-tree: {}", schema.page.page_header.page_type);
            eprintln!("Number of cells: {}", num_cells);

            eprintln!("Page: {:?}", &page1[0..20]);
            let mut offsets: Vec<u16> = Vec::with_capacity(num_cells as usize);

            // for i in 0..num_cells {
            //     let offset_index = SQLITE_PAGE_HEADER_SIZE + (i * 2) as usize;
            //     let offset = u16::from_be_bytes([
            //         page1[offset_index],
            //         page1[offset_index + 1]
            //     ]);
            //     offsets.push(offset - 100);
            // }
            //
            // eprintln!("Offsets: {:?}", offsets);
            //
            // // let size1 = decode_sqlite_varint(&page1[offsets[0] as usize..(offsets[0] + 9) as usize]);
            // // println!("Size: {}", size1);
            // // let size2 = decode_sqlite_varint(&page1[offsets[1] as usize..(offsets[1] + 9) as usize]);
            // // println!("Size: {}", size2);
            // let (size3, size_bytes) = decode_sqlite_varint(&page1[offsets[2] as usize..(offsets[2] + 9) as usize]);
            // println!("Size: {}, {}", size3, size_bytes);
            //
            // let (row_id, size_row) = decode_sqlite_varint(&page1[offsets[2] as usize + size_bytes..(offsets[2] + 9) as usize + size_bytes]);
            // println!("Rowid: {:?}", row_id);
            //
            // let (header_size, header_size_size) = decode_sqlite_varint(&page1[offsets[2] as usize + size_bytes + size_row..(offsets[2] + 9) as usize + size_bytes + size_row]);
            // println!("Header size: {:?}", header_size);
            //
            // let mut record_header = vec![];
            //
            // let mut index = 0;
            // while index < header_size - 1 {
            //     let (size, size_bytes) = decode_sqlite_varint(&page1[offsets[2] as usize + size_bytes + size_row + header_size_size + (index as usize)..(offsets[2] + 9) as usize + size_bytes + size_row + header_size_size + (index as usize)]);
            //     record_header.push(size);
            //     index += size_bytes as u64;
            // }
            //
            // println!("Record header: {:?}", record_header);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
