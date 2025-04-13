#!/bin/bash

echo "--------------------------------------"
echo "WhatQL Comprehensive Test Script (WSL)"
echo "--------------------------------------"

# Set paths
WHATQL_PATH="../target/release/codecrafters-sqlite"
DB_PATH="test_whatql.db"

echo "Creating test database..."
# Check if sqlite3 is installed
if ! command -v sqlite3 &> /dev/null; then
    echo "sqlite3 not found. Installing..."
    sudo apt-get update
    sudo apt-get install -y sqlite3
fi

# Create the database
sqlite3 $DB_PATH < create_test_db.sql

echo
echo "--------------------------------------"
echo "Testing database information commands"
echo "--------------------------------------"
echo

echo "1. Running .dbinfo command:"
$WHATQL_PATH $DB_PATH .dbinfo
echo

echo "2. Running .tables command:"
$WHATQL_PATH $DB_PATH .tables
echo

echo "--------------------------------------"
echo "Testing Basic SQL Queries"
echo "--------------------------------------"
echo

echo "3. Simple SELECT query:"
$WHATQL_PATH $DB_PATH "SELECT * FROM users"
echo

echo "4. SELECT with WHERE clause:"
$WHATQL_PATH $DB_PATH "SELECT name, email FROM users WHERE age > 30"
echo

echo "5. SELECT with ORDER BY:"
$WHATQL_PATH $DB_PATH "SELECT name, age FROM users ORDER BY age DESC"
echo

echo "6. SELECT with LIMIT:"
$WHATQL_PATH $DB_PATH "SELECT * FROM users LIMIT 2"
echo

echo "--------------------------------------"
echo "Testing Joins and Aggregates"
echo "--------------------------------------"
echo

echo "7. INNER JOIN:"
$WHATQL_PATH $DB_PATH "SELECT users.name, orders.product, orders.amount FROM users JOIN orders ON users.id = orders.user_id"
echo

echo "8. LEFT JOIN:"
$WHATQL_PATH $DB_PATH "SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id"
echo

echo "9. GROUP BY with aggregation:"
$WHATQL_PATH $DB_PATH "SELECT users.name, COUNT(orders.id) as order_count, SUM(orders.amount) as total FROM users LEFT JOIN orders ON users.id = orders.user_id GROUP BY users.id"
echo

echo "10. HAVING clause:"
$WHATQL_PATH $DB_PATH "SELECT users.name, COUNT(orders.id) as order_count FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.id HAVING COUNT(orders.id) > 1"
echo

echo "--------------------------------------"
echo "Testing Complex Queries"
echo "--------------------------------------"
echo

echo "11. Subquery in WHERE:"
$WHATQL_PATH $DB_PATH "SELECT name FROM users WHERE id IN (SELECT DISTINCT user_id FROM orders WHERE amount > 50)"
echo

echo "12. Nested joins:"
$WHATQL_PATH $DB_PATH "SELECT u.name, o.product, c.comment FROM users u JOIN orders o ON u.id = o.user_id JOIN comments c ON o.id = c.order_id"
echo

echo "13. UNION:"
$WHATQL_PATH $DB_PATH "SELECT name FROM users WHERE age < 30 UNION SELECT product as name FROM orders WHERE amount > 75"
echo

echo "14. Complex filtering:"
$WHATQL_PATH $DB_PATH "SELECT u.name, o.product, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE u.age > 30 AND o.amount > 30 ORDER BY o.amount DESC"
echo

echo "--------------------------------------"
echo "Testing Schema Modification"
echo "--------------------------------------"
echo

echo "15. CREATE TABLE (if not exists):"
$WHATQL_PATH $DB_PATH "CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, value TEXT)"
echo

echo "16. INSERT:"
$WHATQL_PATH $DB_PATH "INSERT INTO test_table (value) VALUES ('test value 1'), ('test value 2')"
echo

echo "17. UPDATE:"
$WHATQL_PATH $DB_PATH "UPDATE test_table SET value = 'updated value' WHERE id = 1"
echo

echo "18. DELETE:"
$WHATQL_PATH $DB_PATH "DELETE FROM test_table WHERE id = 2"
echo

echo "19. SELECT from modified table:"
$WHATQL_PATH $DB_PATH "SELECT * FROM test_table"
echo

echo "--------------------------------------"
echo "Test Complete"
echo "--------------------------------------"