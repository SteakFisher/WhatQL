use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;

const SQLITE_HEADER_SIZE: usize = 100;
const SQLITE_PAGE_SIZE: usize = 4096;

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
            let mut file = File::open(&args[1])?;
            let mut header = [0; SQLITE_HEADER_SIZE];
            file.read_exact(&mut header)?;

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            let page_size = u16::from_be_bytes([header[16], header[17]]);

            // You can use print statements as follows for debugging, they'll be visible when running tests.
            eprintln!("Logs from your program will appear here!");

            // Uncomment this block to pass the first stage
            println!("database page size: {}", page_size);

            let mut page1 = [0; SQLITE_PAGE_SIZE];
            file.read_exact(&mut page1)?;

            let num_pages = u16::from_be_bytes([page1[3], page1[4]]);

            println!("number of tables: {}", num_pages);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
