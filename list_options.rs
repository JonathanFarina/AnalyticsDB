use datafusion::error::Result;
use datafusion::prelude::*;

// Define a dummy version of base_session_config to test it
fn test_base_session_config() -> SessionConfig {
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = false;
    config.options_mut().sql_parser.map_string_types_to_utf8view = false;
    config
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = test_base_session_config();
    config = config.with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);
    let df = ctx.sql("SHOW ALL").await?;
    df.show_limit(1000).await?;
    Ok(())
}
