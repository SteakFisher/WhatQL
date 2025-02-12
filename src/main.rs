mod helpers;
mod classes;

use crate::helpers::SqliteValue;
use anyhow::{bail, Result};
use classes::Database;
use std::io::prelude::*;
use std::ops::Index;
use crate::classes::RecordType;

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

            let num_pages = page1.page_header.num_cells;

            println!("number of tables: {}", num_pages);
        }
        ".tables" => {
            let db = Database::new(args[1].clone());

            let schema = db.get_schema()?;

            let offsets = &schema.get_cell_contents();

            let offsets = schema.get_cell_offsets();

            for i in 0..offsets.len() {
                let offset = offsets[i];
                let cell = schema.get_cell_content(offset)?;

                if let RecordType::SchemaRecord(record) = &cell.record {
                    if let SqliteValue::Text(text) = &record.values[1] {
                        println!("{}", text);
                    }
                }

                // if let SqliteValue::Text(text) = &cell.record.values[2] {
                //     println!("{}", text);
                // }
            }
        }
        _ => {
            let command_split = command.split(" ").collect::<Vec<&str>>();
            let query_table_name = &command_split[command_split.len() - 1];
            eprintln!("Querying table: {}", query_table_name);

            let db = Database::new(args[1].clone());

            // bail!("Missing or invalid command passed: {}", command.as_str())

        },
    }

    Ok(())
}
