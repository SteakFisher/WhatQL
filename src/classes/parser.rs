use sqlparser::ast::Select;

pub struct SelectParser {}

pub enum SQLParser {
    SelectParser(SelectParser)
}

impl SelectParser {
    pub fn get_columns(select: Box<Select>) -> Vec<String> {
        let mut columns = vec![];

        match &select.projection[0] {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                match expr {
                    sqlparser::ast::Expr::Identifier(ident) => {
                        columns.push(ident.value.clone());
                    }
                    _ => {}
                }
            }
            _ => {}
        }

        columns
    }

    pub fn get_table_names(select: Box<Select>) -> Vec<String> {
        let mut table_names = Vec::new();

        match &select.from[0].relation {
            sqlparser::ast::TableFactor::Table { name, .. } => {
                match name {
                    sqlparser::ast::ObjectName(ident) => {
                        table_names.push(ident[0].value.clone());
                    }
                    _ => {}
                }
            }
            _ => {}
        }

        table_names
    }
}