mod helpers;
mod classes;

use anyhow::{bail, Result};
use classes::Database;
use std::io::prelude::*;
use crate::helpers::SqliteValue;

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
            // let page1 = schema.page.raw_data;

            schema.page.get_cell_offsets();

            let num_cells =schema.page.page_header.num_cells;

            let offsets = schema.page.get_cell_offsets();

            for i in 0..offsets.len() {
                let offset = offsets[i];
                let cell = schema.page.get_cell_content(offset)?;
                if let SqliteValue::Text(text) = &cell.record.values[2] {
                    println!("{}", text);  // Will print just: apples, sqlite_sequence, oranges
                }
            }

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
