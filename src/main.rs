mod helpers;
mod classes;

use crate::helpers::SqliteValue;
use anyhow::{bail, Result};
use classes::Database;
use sqlparser::ast::Statement;
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;
use std::fmt::Display;
use std::io::prelude::*;
use std::ops::Index;
use crate::classes::SelectParser;

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

            let offsets = schema.get_cell_offsets();

            let table_names = schema.get_table_data();

            for i in table_names {
                if let SqliteValue::Text(text) = &i.values[1] {
                    println!("{}", text);
                }
            }
        }
        _ => {
            // let command_split = command.split(" ").collect::<Vec<&str>>();
            // let query_table_name = &command_split[command_split.len() - 1];
            // eprintln!("Querying table: {}", query_table_name);
            //
            // let db = Database::new(args[1].clone());
            //
            // let schema = db.get_schema()?;
            //
            //
            // let table_data = schema.get_table_data();
            //
            // let mut table_found = false;
            // let mut table_root_page = 0;
            //
            // for i in table_data {
            //     if let SqliteValue::Text(text) = &i.values[1] {
            //         if (text == query_table_name) {
            //             table_found = true;
            //             if let SqliteValue::Integer(root_page_number) = &i.values[3] {
            //                 if let SqliteValue::Integer(root_page_number) = &i.values[3] {
            //                     table_root_page = *root_page_number as u32;
            //                 }
            //             }
            //         }
            //     }
            // }
            //
            // if !table_found {
            //     bail!("Table not found: {}", query_table_name);
            // }
            //
            // let table = db.get_page(table_root_page - 1)?;
            //
            // let cell_count = table.page_header.num_cells;
            // println!("{}", cell_count);

            // let mut table = schema.get_page(query_table_name)?;
            // bail!("Missing or invalid command passed: {}", command.as_str())

            let dialect = SQLiteDialect {};

            let ast = Parser::parse_sql(&dialect, command)?;

            let mut extracted_column_names: Vec<String> = vec![];
            let mut extracted_table_names: Vec<String> = vec![];

            for statement in ast {
                match statement {
                    Statement::Query(query) => {
                        let query = *query;
                        println!("{:?}", query.body);
                        match *query.body {
                            sqlparser::ast::SetExpr::Select(select) => {
                                extracted_column_names = SelectParser::get_columns(select.clone() );

                                extracted_table_names = SelectParser::get_table_names(select.clone());

                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }

                // Use the built-in `to_string` method for pretty printing
                // println!("{:?}", statement);
            }

            println!("{:?}", extracted_column_names);
            println!("{:?}", extracted_table_names);

            let db = Database::new(args[1].clone());
            let page_num = db.get_page_number(extracted_table_names[0].clone())?;
            let table = db.get_page(page_num - 1)?;
            let smth = table.get_columns();
            println!("{:?}", smth);
        },
    }

    Ok(())
}
