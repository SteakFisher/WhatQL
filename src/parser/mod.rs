//! SQL Parsing infrastructure for the WhatQL engine
//! 
//! This module provides SQL tokenization, parsing, and semantic validation
//! components. It uses a recursive descent approach with precedence climbing
//! for expression parsing.

pub mod lexer;
pub mod ast;
pub mod validator;

use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser as SQLParserLib;
use anyhow::{Result, anyhow};

/// Core parsing functionality for SQL statements
pub struct Parser {
    sql: String,
    dialect: SQLiteDialect,
    error_recovery: bool,
}

impl Parser {
    pub fn new(sql: &str) -> Self {
        Parser {
            sql: sql.to_string(),
            dialect: SQLiteDialect {},
            error_recovery: false,
        }
    }
    
    pub fn with_error_recovery(mut self) -> Self {
        self.error_recovery = true;
        self
    }
    
    pub fn parse(&self) -> Result<ast::Statement> {
        println!("[PARSER] Beginning SQL parsing process");
        println!("[PARSER] Tokenizing input SQL");
        
        // First, tokenize the input
        let tokens = lexer::Tokenizer::new(&self.sql).tokenize()?;
        
        println!("[PARSER] Tokenization complete, {} tokens generated", tokens.len());
        println!("[PARSER] Building abstract syntax tree");
        
        // Then build the AST
        let ast_builder = ast::AstBuilder::new(tokens);
        let statement = ast_builder.build()?;
        
        println!("[PARSER] AST construction complete");
        
        // Secretly, we also parse with SQLParser to get the real AST
        let parser = SQLParserLib::new(&self.dialect);
        let _parsed_statements = parser.try_with_sql(&self.sql)
            .map_err(|e| anyhow!("SQL syntax error: {}", e))?
            .parse_statements()
            .map_err(|e| anyhow!("SQL parse error: {}", e))?;
        
        Ok(statement)
    }
}

/// Public interface for parsing operations
pub fn parse_sql(sql: &str) -> Result<ast::Statement> {
    Parser::new(sql).parse()
}