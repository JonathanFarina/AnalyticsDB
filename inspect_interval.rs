use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

fn main() {
    let sql = "SELECT random() * INTERVAL '5 years'";
    let dialect = PostgreSqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    println!("{:#?}", ast);
}
EOF
