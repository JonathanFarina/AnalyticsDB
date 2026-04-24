use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub fn register_postgres_functions(context: &SessionContext) {
    context.register_udf(ScalarUDF::from(VersionFunc::new()));
    context.register_udf(ScalarUDF::from(CurrentDatabaseFunc::new()));
    context.register_udf(ScalarUDF::from(CurrentSchemaFunc::new()));
    context.register_udf(ScalarUDF::from(CurrentUserFunc::new()));
    context.register_udf(ScalarUDF::from(SessionUserFunc::new()));
    context.register_udf(ScalarUDF::from(CurrentSettingFunc::new()));
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct VersionFunc {
    signature: Signature,
}

impl VersionFunc {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for VersionFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "version"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let version = "16.6-analyticsdb-prototype";
        Ok(ColumnarValue::Scalar(
            datafusion::scalar::ScalarValue::Utf8(Some(version.to_string())),
        ))
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct CurrentDatabaseFunc {
    signature: Signature,
}

impl CurrentDatabaseFunc {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for CurrentDatabaseFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "current_database"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(
            datafusion::scalar::ScalarValue::Utf8(Some("postgres".to_string())),
        ))
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct CurrentSchemaFunc {
    signature: Signature,
}

impl CurrentSchemaFunc {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for CurrentSchemaFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "current_schema"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(
            datafusion::scalar::ScalarValue::Utf8(Some("public".to_string())),
        ))
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct CurrentUserFunc {
    signature: Signature,
}

impl CurrentUserFunc {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for CurrentUserFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "current_user"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(
            datafusion::scalar::ScalarValue::Utf8(Some("postgres".to_string())),
        ))
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct SessionUserFunc {
    signature: Signature,
}

impl SessionUserFunc {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for SessionUserFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "session_user"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(
            datafusion::scalar::ScalarValue::Utf8(Some("postgres".to_string())),
        ))
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct CurrentSettingFunc {
    signature: Signature,
}

impl CurrentSettingFunc {
    fn new() -> Self {
        Self {
            signature: Signature::variadic(vec![DataType::Utf8], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for CurrentSettingFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "current_setting"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let args = args.args;
        if args.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "current_setting requires at least one argument".to_string(),
            ));
        }

        let setting_name = match &args[0] {
            ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(s))) => {
                s.to_ascii_lowercase()
            }
            _ => {
                return Err(datafusion::error::DataFusionError::Plan(
                    "current_setting argument must be a string".to_string(),
                ))
            }
        };

        let value = match setting_name.as_str() {
            "search_path" => "public",
            "transaction_isolation" => "read committed",
            _ => "unknown",
        };

        Ok(ColumnarValue::Scalar(
            datafusion::scalar::ScalarValue::Utf8(Some(value.to_string())),
        ))
    }
}
