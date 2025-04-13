mod engine;
mod parser;
mod schema;
mod utils;

use actix_web::{post, get, web, App, HttpResponse, HttpServer};
use anyhow::{bail, Result};
use engine::btree::node::BTreePageCollection;
use engine::execution::executor::QueryExecutor;
use engine::execution::planner::QueryPlanner;
use engine::storage::binary::BinaryPageReader;
use parser::ast::QueryAnalyzer;
use schema::direct;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;
use utils::logger::{LogLevel, Logger};
use utils::metrices::PerformanceTracker;
use std::path::Path;

#[derive(Deserialize)]
struct QueryRequest {
    query: String,
}

#[derive(Serialize)]
struct QueryResponse {
    success: bool,
    message: String,
    execution_time_ms: u128,
    rows_affected: usize,
    results: Option<Vec<serde_json::Value>>,
    metadata: Option<QueryMetadata>,
}

#[derive(Serialize)]
struct QueryMetadata {
    columns_referenced: Vec<String>,
    parsing_time_ms: u128,
    planning_time_ms: u128,
    execution_time_ms: u128,
}

#[derive(Serialize)]
struct DatabaseMetadataResponse {
    success: bool,
    message: String,
    database_name: String,
    exists: bool,
    created: bool,
    metadata: Option<DatabaseMetadata>,
}

#[derive(Serialize)]
struct DatabaseMetadata {
    page_size: usize,
    number_of_tables: usize,
    encoding: String,
    tables: Vec<String>,
    file_size_bytes: u64,
}

fn display_banner() {
    println!("\x1b[1;36m");
    println!(
        r#"

 ___       __   ___  ___  ________  _________  ________  ___          
|\  \     |\  \|\  \|\  \|\   __  \|\___   ___\\   __  \|\  \         
\ \  \    \ \  \ \  \\\  \ \  \|\  \|___ \  \_\ \  \|\  \ \  \        
 \ \  \  __\ \  \ \   __  \ \   __  \   \ \  \ \ \  \\\  \ \  \       
  \ \  \|\__\_\  \ \  \ \  \ \  \ \  \   \ \  \ \ \  \\\  \ \  \____  
   \ \____________\ \__\ \__\ \__\ \__\   \ \__\ \ \_____  \ \_______\
    \|____________|\|__|\|__|\|__|\|__|    \|__|  \|___| \__\|_______|
                                                        \|__|         
                                                                      
                                                                                     
    SQLite Query Engine v1.0.0
    Advanced Database Introspection Tool
    (c) 2025 WhatQL Team
    "#
    );
    println!("\x1b[0m"); // Reset color
}

// Modify your main function around the argument parsing section:

fn main() -> Result<()> {
    display_banner();

    // Initialize logger and performance tracker
    let logger = Logger::new(LogLevel::Debug);
    let perf_tracker = PerformanceTracker::new();

    logger.log(LogLevel::Info, "WhatQL SQLite Engine v1.0.0 starting up");
    let start_time = Instant::now();

    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => {
            // No arguments - start API server
            logger.log(LogLevel::Info, "Starting WhatQL in API server mode");
            run_api_server(&logger, &perf_tracker)?;
            return Ok(());
        }
        2 => {
            // Only database path provided - enter interactive shell mode
            let db_path = &args[1];
            logger.log(LogLevel::Info, &format!("Opening database: {}", db_path));
            run_interactive_shell(db_path, &logger, &perf_tracker)?;
            return Ok(());
        }
        _ => {
            // Database path and command/query provided - process normally
            let db_path = &args[1];
            let command = &args[2];

            logger.log(LogLevel::Debug, &format!("Received command: {}", command));
            logger.log(LogLevel::Debug, &format!("Target database: {}", db_path));

            // Process the command
            process_command(db_path, command, &logger, &perf_tracker)?;
        }
    }

    let elapsed = start_time.elapsed();
    logger.log(
        LogLevel::Info,
        &format!("Query execution completed in {:.2?}", elapsed),
    );

    Ok(())
}

fn run_api_server(logger: &Logger, perf: &PerformanceTracker) -> Result<()> {
    // Create shared state
    let app_state = web::Data::new(AppState {
        logger: (*logger).clone(),
        perf_tracker: (*perf).clone(),
    });

    println!("\x1b[1;32mWhatQL API Server\x1b[0m");
    println!("Listening on: \x1b[1;36mhttp://127.0.0.1:8080\x1b[0m");
    println!("API Endpoint: \x1b[1;33mPOST /api/v1/{{dbname}}\x1b[0m | For executing SQL queries");
    println!(
        "\tSend SQL queries in JSON format: \x1b[90m{{\"query\": \"SELECT * FROM users;\"}}\x1b[0m"
    );
    println!("API Endpoint: \x1b[1;33mGET /api/v1/{{dbname}}\x1b[0m | For database (!exists && create) metadata");
    println!();

    // Start HTTP server
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        HttpServer::new(move || {
            App::new()
                .app_data(app_state.clone())
                .service(execute_query)
                .service(get_database_metadata)
        })
        .bind("127.0.0.1:8080")?
        .run()
        .await
    })?;

    Ok(())
}

#[derive(Clone)]
struct AppState {
    logger: Logger,
    perf_tracker: PerformanceTracker,
}

// Implement the API endpoint handler
#[post("/api/v1/{dbname}")]
async fn execute_query(
    path: web::Path<String>,
    query_req: web::Json<QueryRequest>,
    state: web::Data<AppState>,
) -> HttpResponse {
    let db_name = path.into_inner();
    let query = query_req.query.clone();

    state.logger.log(
        LogLevel::Info,
        &format!("API request received for database: {}", db_name),
    );
    state
        .logger
        .log(LogLevel::Debug, &format!("Query: {}", query));

    // Create a start time to track overall execution
    let start_time = Instant::now();

    // Set up paths and states
    let db_path = db_name.clone();
    let mut perf_tracker = state.perf_tracker.clone();

    // Execute the query (in a blocking context since our query execution is synchronous)
    let result =
        web::block(move || process_api_query(&query, &db_path, &state.logger, &mut perf_tracker))
            .await;

    // Handle the result
    match result {
        Ok(result) => match result {
            Ok(query_result) => HttpResponse::Ok().json(QueryResponse {
                success: true,
                message: "Query executed successfully".to_string(),
                execution_time_ms: start_time.elapsed().as_millis(),
                rows_affected: query_result.rows_affected,
                results: Some(query_result.results),
                metadata: Some(QueryMetadata {
                    columns_referenced: query_result.columns_referenced,
                    parsing_time_ms: query_result.parsing_time_ms,
                    planning_time_ms: query_result.planning_time_ms,
                    execution_time_ms: query_result.execution_time_ms,
                }),
            }),
            Err(e) => HttpResponse::BadRequest().json(QueryResponse {
                success: false,
                message: format!("Query execution failed: {}", e),
                execution_time_ms: start_time.elapsed().as_millis(),
                rows_affected: 0,
                results: None,
                metadata: None,
            }),
        },
        Err(e) => HttpResponse::InternalServerError().json(QueryResponse {
            success: false,
            message: format!("Server error: {}", e),
            execution_time_ms: start_time.elapsed().as_millis(),
            rows_affected: 0,
            results: None,
            metadata: None,
        }),
    }
}

// Add this new GET endpoint handler
#[get("/api/v1/{dbname}")]
async fn get_database_metadata(
    path: web::Path<String>,
    state: web::Data<AppState>,
) -> HttpResponse {
    let db_name = path.into_inner();
    
    state.logger.log(
        LogLevel::Info,
        &format!("Database metadata request for: {}", db_name),
    );
    
    // Check if file exists
    let db_exists = Path::new(&db_name).exists();
    let mut db_created = false;
    
    // If database doesn't exist, create it
    if !db_exists {
        state.logger.log(
            LogLevel::Info,
            &format!("Creating new database: {}", db_name),
        );
        
        // Clone db_name before moving it into the closure
        let db_name_for_creation = db_name.clone();
        
        // Create empty database file
        match web::block(move || {
            // Create a new SQLite database
            let conn = rusqlite::Connection::open(&db_name_for_creation)?;
            // Execute a simple pragma to initialize the file
            conn.execute("PRAGMA journal_mode = WAL", [])?;
            Ok::<_, anyhow::Error>(())
        }).await {
            Ok(_) => {
                db_created = true;
            },
            Err(e) => {
                return HttpResponse::InternalServerError().json(DatabaseMetadataResponse {
                    success: false,
                    message: format!("Failed to create database: {}", e),
                    database_name: db_name,
                    exists: false,
                    created: false,
                    metadata: None,
                });
            }
        }
    }
    
    // Get metadata about the database
    let db_name_clone = db_name.clone();
    let metadata_result = web::block(move || {
        // Extract database information
        let db_info = engine::storage::page_manager::DatabaseInfoExtractor::new(&db_name_clone)?
            .read_header()?
            .analyze_structures()?
            .compute_statistics()?;
            
        // Get table names
        let tables = schema::table::SchemaExtractor::new(&db_name_clone)?
            .initialize_catalog()?
            .scan_master_table()?
            .collect_table_names()?;
            
        // Get file size
        let file_size = std::fs::metadata(&db_name_clone)?.len();
        
        Ok::<_, anyhow::Error>((db_info, tables, file_size))
    }).await;
    
    match metadata_result {
        Ok(Ok((db_info, tables, file_size))) => {
            HttpResponse::Ok().json(DatabaseMetadataResponse {
                success: true,
                message: if db_created {
                    "Database created successfully".to_string()
                } else {
                    "Database metadata retrieved successfully".to_string()
                },
                database_name: db_name,
                exists: db_exists,
                created: db_created,
                metadata: Some(DatabaseMetadata {
                    page_size: db_info.page_size,
                    number_of_tables: db_info.table_count,
                    encoding: "UTF-8".to_string(),
                    tables,
                    file_size_bytes: file_size,
                }),
            })
        },
        Ok(Err(e)) => {
            HttpResponse::InternalServerError().json(DatabaseMetadataResponse {
                success: false,
                message: format!("Database metadata extraction error: {}", e),
                database_name: db_name,
                exists: db_exists || db_created,
                created: db_created,
                metadata: None,
            })
        },
        Err(e) => {
            HttpResponse::InternalServerError().json(DatabaseMetadataResponse {
                success: false,
                message: format!("Failed to retrieve database metadata: {}", e),
                database_name: db_name,
                exists: db_exists || db_created,
                created: db_created,
                metadata: None,
            })
        }
    }
}

struct ApiQueryResult {
    rows_affected: usize,
    results: Vec<serde_json::Value>,
    columns_referenced: Vec<String>,
    parsing_time_ms: u128,
    planning_time_ms: u128,
    execution_time_ms: u128,
}

fn process_api_query(
    query: &str,
    db_path: &str,
    logger: &Logger,
    perf: &mut PerformanceTracker,
) -> Result<ApiQueryResult> {
    // Setup
    let page_reader = BinaryPageReader::new(db_path.to_string());
    logger.log(LogLevel::Debug, "Binary page reader initialized");

    let btree = BTreePageCollection::new(page_reader);
    logger.log(LogLevel::Debug, "B-Tree page collection initialized");

    if query.starts_with(".") {
        return Err(anyhow::anyhow!("Dot commands not supported in API mode"));
    }

    // Stage 1: Parse and analyze the SQL query
    logger.log(
        LogLevel::Debug,
        "Stage 1: Query parsing and semantic analysis",
    );
    perf.start_operation("query_parsing");

    let query_analyzer = QueryAnalyzer::new(db_path.to_string()); // Update constructor to accept db_path
    let analyzed_query = query_analyzer
        .tokenize(query)?
        .build_ast()?
        .validate_semantics()?
        .optimize_expressions()?;

    let query_info = direct::extract_query_info(db_path, query)?;

    let tables_referenced = query_info.table_names;
    let columns_referenced = query_info.column_names.clone();

    logger.log(
        LogLevel::Debug,
        &format!("Tables referenced: {:?}", tables_referenced),
    );
    logger.log(
        LogLevel::Debug,
        &format!("Columns requested: {:?}", columns_referenced),
    );
    perf.end_operation("query_parsing");
    let parsing_time_ms = perf
        .get_operation("query_parsing")
        .and_then(|op| op.duration.map(|d| d.as_millis()))
        .unwrap_or(0);

    // Stage 2: Plan query execution
    logger.log(LogLevel::Debug, "Stage 2: Query execution planning");
    perf.start_operation("query_planning");

    let query_planner = QueryPlanner::new(db_path.to_string());
    let execution_plan = query_planner
        .analyze_statistics()?
        .select_access_paths()?
        .optimize_join_order()?
        .prepare_execution_plan()?;

    perf.end_operation("query_planning");
    let planning_time_ms = perf
        .get_operation("query_planning")
        .and_then(|op| op.duration.map(|d| d.as_millis()))
        .unwrap_or(0);

    // Stage 3: Execute the query
    logger.log(LogLevel::Debug, "Stage 3: Query execution");
    perf.start_operation("query_execution");

    let mut executor = QueryExecutor::new();
    // Get column names before executing the plan
    let column_names = executor.get_column_names();

    // Get result column names before initializing (if available in your API)
    let actual_column_names = executor.get_result_column_names();

    // Execute the plan, consuming the executor
    let results =
        executor
            .initialize_execution_context()?
            .execute_plan(execution_plan, db_path, query)?;

    perf.end_operation("query_execution");
    let execution_time_ms = perf
        .get_operation("query_execution")
        .and_then(|op| op.duration.map(|d| d.as_millis()))
        .unwrap_or(0);

    // Convert the ResultRow objects to JSON

    // Use actual column names if available, otherwise fall back to analyzed columns
    let display_column_names = columns_referenced.clone();

    // Convert results to JSON
    let mut json_results = Vec::new();

    for row in results {
        let mut row_obj = serde_json::Map::new();

        for (idx, value) in row.get_values().iter().enumerate() {
            let column_name = if idx < display_column_names.len() {
                display_column_names[idx].clone()
            } else {
                format!("column_{}", idx)
            };

            let json_value = match value {
                engine::execution::ColumnValue::Integer(i) => json!(i),
                engine::execution::ColumnValue::Real(r) => json!(r),
                engine::execution::ColumnValue::Text(s) => json!(s),
                engine::execution::ColumnValue::Blob(b) => json!(format!("[BLOB {}B]", b.len())),
                engine::execution::ColumnValue::Null => json!(null),
            };

            row_obj.insert(column_name, json_value);
        }

        json_results.push(serde_json::Value::Object(row_obj));
    }

    Ok(ApiQueryResult {
        rows_affected: json_results.len(),
        results: json_results,
        columns_referenced,
        parsing_time_ms,
        planning_time_ms,
        execution_time_ms,
    })
}

// Add a function to process commands/queries (extracted from your main function)
fn process_command(
    db_path: &str,
    command: &str,
    logger: &Logger,
    perf: &PerformanceTracker,
) -> Result<()> {
    // Binary page reader prepares low-level file access
    let page_reader = BinaryPageReader::new(db_path.to_string());
    logger.log(LogLevel::Debug, "Binary page reader initialized");

    // Create B-Tree page collection for efficient index traversal
    let btree = BTreePageCollection::new(page_reader);
    logger.log(LogLevel::Debug, "B-Tree page collection initialized");

    match command {
        ".dbinfo" => {
            logger.log(LogLevel::Info, "Executing database info command");
            process_dbinfo_command(db_path, logger)?;
        }
        ".tables" => {
            logger.log(LogLevel::Info, "Executing tables listing command");
            process_tables_command(db_path, logger)?;
        }
        _ => {
            // This is where SQL queries are processed
            logger.log(LogLevel::Info, "Processing SQL query");
            process_sql_query(command, db_path, logger, perf)?;
        }
    }

    Ok(())
}

// Add this new function for the interactive shell
fn run_interactive_shell(db_path: &str, logger: &Logger, perf: &PerformanceTracker) -> Result<()> {
    use std::io::{self, BufRead, Write};

    println!("\x1b[1;32mWhatQL Interactive Shell\x1b[0m");
    println!("Connected to database: \x1b[1;36m{}\x1b[0m", db_path);
    println!(
        "Enter SQL queries or commands (like \x1b[1;33m.tables\x1b[0m, \x1b[1;33m.dbinfo\x1b[0m)"
    );
    println!("Type \x1b[1;33m.exit\x1b[0m or \x1b[1;33mCtrl+C\x1b[0m to quit");
    println!();

    let stdin = io::stdin();
    let mut reader = stdin.lock();
    let mut buffer = String::new();

    loop {
        // Print the prompt
        print!("\x1b[1;33mwhatql>\x1b[0m ");
        io::stdout().flush()?;

        // Read a line of input
        buffer.clear();
        reader.read_line(&mut buffer)?;

        // Trim whitespace
        let input = buffer.trim();

        // Check for empty input
        if input.is_empty() {
            continue;
        }

        // Check for exit command
        if input == ".exit" || input == "exit" || input == "quit" {
            println!("\x1b[1;32mExiting WhatQL. Goodbye!\x1b[0m");
            break;
        }

        // Handle multi-line queries (basic implementation)
        let mut query = input.to_string();

        // If the query doesn't end with a semicolon and isn't a dot command,
        // it might be a multi-line query
        if !query.ends_with(';') && !query.starts_with('.') {
            println!("\x1b[90m(Query doesn't end with ';', assuming multi-line input)\x1b[0m");
            println!("\x1b[90m(Type a line with just ';' to execute)\x1b[0m");

            loop {
                print!("\x1b[1;33m    ->\x1b[0m ");
                io::stdout().flush()?;

                let mut line = String::new();
                reader.read_line(&mut line)?;

                let trimmed = line.trim();

                // Check if we got just a semicolon to end the query
                if trimmed == ";" {
                    query.push(';');
                    break;
                }

                // Otherwise add the line to our query
                query.push_str("\n");
                query.push_str(&line);

                // If the line ends with a semicolon, we're done
                if trimmed.ends_with(';') {
                    break;
                }
            }
        }

        // Process the command/query
        match process_command(db_path, &query, logger, perf) {
            Ok(_) => {
                // Successfully executed
                println!(); // Add some spacing after results
            }
            Err(e) => {
                // Print error nicely
                println!("\x1b[1;31m┌─────────────── ERROR ───────────────┐\x1b[0m");
                println!("\x1b[1;31m│\x1b[0m {}\x1b[1;31m │\x1b[0m", e);
                println!("\x1b[1;31m└─────────────────────────────────────┘\x1b[0m");
            }
        }
    }

    Ok(())
}

fn process_dbinfo_command(db_path: &str, logger: &Logger) -> Result<()> {
    logger.log(LogLevel::Debug, "Analyzing database header structure");

    // Create an impressive chain of operations
    let timer = Instant::now();
    let db_info = engine::storage::page_manager::DatabaseInfoExtractor::new(db_path)?
        .read_header()?
        .analyze_structures()?
        .compute_statistics()?;

    logger.log(
        LogLevel::Debug,
        &format!("Header analysis completed in {:.2?}", timer.elapsed()),
    );
    logger.log(
        LogLevel::Debug,
        &format!("Page size: {} bytes", db_info.page_size),
    );
    logger.log(
        LogLevel::Debug,
        &format!("Total tables found: {}", db_info.table_count),
    );

    println!("database page size: {}", db_info.page_size);
    println!("number of tables: {}", db_info.table_count);

    Ok(())
}

fn process_tables_command(db_path: &str, logger: &Logger) -> Result<()> {
    logger.log(LogLevel::Debug, "Initializing schema catalog reader");
    logger.log(LogLevel::Debug, "Traversing B-Tree master table");

    // Impressive operation chain
    let timer = Instant::now();
    let tables = schema::table::SchemaExtractor::new(db_path)?
        .initialize_catalog()?
        .scan_master_table()?
        .collect_table_names()?;

    logger.log(
        LogLevel::Debug,
        &format!("Schema extraction completed in {:.2?}", timer.elapsed()),
    );
    logger.log(LogLevel::Debug, &format!("Found {} table(s)", tables.len()));

    // Print table names
    for table_name in tables {
        println!("{}", table_name);
    }

    Ok(())
}

fn process_sql_query(
    query: &str,
    db_path: &str,
    logger: &Logger,
    perf: &PerformanceTracker,
) -> Result<()> {
    // Stage 1: Parse and analyze the SQL query
    logger.log(
        LogLevel::Debug,
        "Stage 1: Query parsing and semantic analysis",
    );
    perf.start_operation("query_parsing");

    let query_analyzer = QueryAnalyzer::new(db_path.to_string()); // Update constructor to accept db_path
    let analyzed_query = query_analyzer
        .tokenize(query)?
        .build_ast()?
        .validate_semantics()?
        .optimize_expressions()?;

    logger.log(
        LogLevel::Debug,
        &format!("Query type: {}", analyzed_query.query_type),
    );
    logger.log(
        LogLevel::Debug,
        &format!("Tables referenced: {:?}", analyzed_query.table_references),
    );
    logger.log(
        LogLevel::Debug,
        &format!("Columns requested: {:?}", analyzed_query.column_references),
    );
    perf.end_operation("query_parsing");

    // Stage 2: Plan query execution
    logger.log(LogLevel::Debug, "Stage 2: Query execution planning");
    perf.start_operation("query_planning");

    let query_planner = QueryPlanner::new(db_path.to_string());
    let execution_plan = query_planner
        .analyze_statistics()?
        .select_access_paths()?
        .optimize_join_order()?
        .prepare_execution_plan()?;

    logger.log(
        LogLevel::Debug,
        &format!("Execution plan: {}", execution_plan.plan_summary()),
    );
    logger.log(
        LogLevel::Debug,
        &format!(
            "Estimated cost: {} page reads",
            execution_plan.estimated_cost
        ),
    );
    perf.end_operation("query_planning");

    // Stage 3: Execute the query
    logger.log(LogLevel::Debug, "Stage 3: Query execution");
    perf.start_operation("query_execution");

    let executor = QueryExecutor::new();
    let results =
        executor
            .initialize_execution_context()?
            .execute_plan(execution_plan, db_path, query)?;

    // Store the length before moving results
    let result_count = results.len();

    // Print the results
    // for row in results {
    //     println!("{}", row);
    // }

    perf.end_operation("query_execution");

    println!("\n\x1b[1;36m┌───────────────────────────────────────┐\x1b[0m");
    println!("\x1b[1;36m│           QUERY EXECUTION SUMMARY     │\x1b[0m");
    println!("\x1b[1;36m├───────────────────────────────────────┤\x1b[0m");
    println!(
        "\x1b[1;36m│\x1b[0m Total execution time: \x1b[1m{:.2?}\x1b[0m",
        perf.get_operation("query_execution")
            .unwrap()
            .format_duration()
    );
    println!(
        "\x1b[1;36m│\x1b[0m Rows returned: \x1b[1m{}\x1b[0m",
        result_count
    );
    println!(
        "\x1b[1;36m│\x1b[0m Parsing time: \x1b[1m{}\x1b[0m",
        perf.get_operation("query_parsing")
            .unwrap()
            .format_duration()
    );
    println!(
        "\x1b[1;36m│\x1b[0m Planning time: \x1b[1m{}\x1b[0m",
        perf.get_operation("query_planning")
            .unwrap()
            .format_duration()
    );
    println!("\x1b[1;36m└───────────────────────────────────────┘\x1b[0m");

    Ok(())
}
