use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, IntervalMonthDayNanoArray};
use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNanoType};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;

pub fn register_postgres_functions(context: &SessionContext) {
    context.register_udf(ScalarUDF::from(VersionFunc::new()));
    context.register_udf(ScalarUDF::from(CurrentDatabaseFunc::new()));
    context.register_udf(ScalarUDF::from(CurrentSchemaFunc::new()));
    context.register_udf(ScalarUDF::from(CurrentUserFunc::new()));
    context.register_udf(ScalarUDF::from(SessionUserFunc::new()));
    context.register_udf(ScalarUDF::from(CurrentSettingFunc::new()));
    context.register_udf(ScalarUDF::from(FormatTypeFunc::new()));
    context.register_udf(ScalarUDF::from(ObjDescriptionFunc::new()));
    context.register_udf(ScalarUDF::from(ColDescriptionFunc::new()));

    // pg_catalog qualified UDFs for explicit client calls
    context.register_udf(ScalarUDF::from(PgGetExprFunc::new()));
    context.register_udf(ScalarUDF::from(PgGetPartkeydefFunc::new()));
    context.register_udf(ScalarUDF::from(PgGetConstraintdefFunc::new()));

    // AnalyticsDB custom functions for PostgreSQL parity
    context.register_udf(ScalarUDF::from(MultiplyIntervalFunc::new()));
    context.register_udf(ScalarUDF::from(DivideIntervalFunc::new()));

    // Multipart name registration for schema-qualified calls
    context.register_udf(ScalarUDF::new_from_impl(PgGetExprFunc::with_name(
        "pg_catalog.pg_get_expr",
    )));
    context.register_udf(ScalarUDF::new_from_impl(PgGetPartkeydefFunc::with_name(
        "pg_catalog.pg_get_partkeydef",
    )));
    context.register_udf(ScalarUDF::new_from_impl(PgGetConstraintdefFunc::with_name(
        "pg_catalog.pg_get_constraintdef",
    )));
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
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            version.to_string(),
        ))))
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
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "postgres".to_string(),
        ))))
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
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "public".to_string(),
        ))))
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
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "postgres".to_string(),
        ))))
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
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "postgres".to_string(),
        ))))
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
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.to_ascii_lowercase(),
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

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            value.to_string(),
        ))))
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct FormatTypeFunc {
    signature: Signature,
}

impl FormatTypeFunc {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FormatTypeFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "format_type"
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
                "format_type requires at least one argument".to_string(),
            ));
        }

        match &args[0] {
            ColumnarValue::Scalar(scalar) => {
                let type_name = match scalar {
                    ScalarValue::UInt32(Some(oid)) => oid_to_type_name(*oid),
                    ScalarValue::Int32(Some(oid)) => oid_to_type_name(*oid as u32),
                    ScalarValue::Int64(Some(oid)) => oid_to_type_name(*oid as u32),
                    ScalarValue::UInt64(Some(oid)) => oid_to_type_name(*oid as u32),
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                        s.split('.').last().unwrap_or(s).to_string()
                    }
                    _ => "unknown".to_string(),
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(type_name))))
            }
            ColumnarValue::Array(array) => {
                let mut results = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    let scalar = ScalarValue::try_from_array(array, i)?;
                    let type_name = match scalar {
                        ScalarValue::UInt32(Some(oid)) => oid_to_type_name(oid),
                        ScalarValue::Int32(Some(oid)) => oid_to_type_name(oid as u32),
                        ScalarValue::Int64(Some(oid)) => oid_to_type_name(oid as u32),
                        ScalarValue::UInt64(Some(oid)) => oid_to_type_name(oid as u32),
                        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                            s.split('.').last().unwrap_or(&s).to_string()
                        }
                        _ => "unknown".to_string(),
                    };
                    results.push(Some(type_name));
                }
                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }
        }
    }
}

fn oid_to_type_name(oid: u32) -> String {
    let name = match oid {
        16 => "boolean",
        18 => "char",
        20 => "bigint",
        21 => "smallint",
        23 => "integer",
        25 => "text",
        700 => "real",
        701 => "double precision",
        1114 => "timestamp without time zone",
        1184 => "timestamp with time zone",
        _ => "unknown",
    };
    name.to_string()
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct ObjDescriptionFunc {
    signature: Signature,
}

impl ObjDescriptionFunc {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ObjDescriptionFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "obj_description"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct ColDescriptionFunc {
    signature: Signature,
}

impl ColDescriptionFunc {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ColDescriptionFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "col_description"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct PgGetExprFunc {
    name: String,
    signature: Signature,
}

impl PgGetExprFunc {
    fn new() -> Self {
        Self {
            name: "pg_get_expr".to_string(),
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }

    fn with_name(name: &str) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for PgGetExprFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
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
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        }
        // Just return the first arg as text if possible
        match &args[0] {
            ColumnarValue::Scalar(s) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                s.to_string(),
            )))),
            ColumnarValue::Array(a) => {
                let mut results = Vec::with_capacity(a.len());
                for i in 0..a.len() {
                    results.push(Some(ScalarValue::try_from_array(a, i)?.to_string()));
                }
                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::StringArray::from(results),
                )))
            }
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct PgGetPartkeydefFunc {
    name: String,
    signature: Signature,
}

impl PgGetPartkeydefFunc {
    fn new() -> Self {
        Self {
            name: "pg_get_partkeydef".to_string(),
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }

    fn with_name(name: &str) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for PgGetPartkeydefFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct PgGetConstraintdefFunc {
    name: String,
    signature: Signature,
}

impl PgGetConstraintdefFunc {
    fn new() -> Self {
        Self {
            name: "pg_get_constraintdef".to_string(),
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }

    fn with_name(name: &str) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for PgGetConstraintdefFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct MultiplyIntervalFunc {
    signature: Signature,
}

impl MultiplyIntervalFunc {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),
                    DataType::Float64,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for MultiplyIntervalFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "multiply_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Interval(
            datafusion::arrow::datatypes::IntervalUnit::MonthDayNano,
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let args = args.args;
        match (&args[0], &args[1]) {
            (ColumnarValue::Scalar(s), ColumnarValue::Scalar(f)) => {
                let interval = match s {
                    ScalarValue::IntervalMonthDayNano(Some(v)) => *v,
                    _ => {
                        return Err(datafusion::error::DataFusionError::Execution(format!(
                            "Expected interval, found {:?}",
                            s
                        )))
                    }
                };
                let factor = match f {
                    ScalarValue::Float64(Some(v)) => *v,
                    ScalarValue::Int64(Some(v)) => *v as f64,
                    ScalarValue::Decimal128(Some(v), _, scale) => {
                        *v as f64 / 10f64.powi(*scale as i32)
                    }
                    _ => {
                        return Err(datafusion::error::DataFusionError::Execution(format!(
                            "Expected numeric factor, found {:?}",
                            f
                        )))
                    }
                };

                let (months, days, nanoseconds) = IntervalMonthDayNanoType::to_parts(interval);
                let months = (months as f64 * factor) as i32;
                let days = (days as f64 * factor) as i32;
                let nanos = (nanoseconds as f64 * factor) as i64;

                Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                    Some(IntervalMonthDayNanoType::make_value(months, days, nanos)),
                )))
            }
            (ColumnarValue::Array(s), ColumnarValue::Array(f)) => {
                let s = s
                    .as_any()
                    .downcast_ref::<IntervalMonthDayNanoArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "Expected IntervalMonthDayNanoArray".to_string(),
                        )
                    })?;
                let f = f.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "Expected Float64Array".to_string(),
                    )
                })?;

                if s.len() != f.len() {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "Mismatched array lengths in multiply_interval".to_string(),
                    ));
                }

                let mut results = Vec::with_capacity(s.len());
                for i in 0..s.len() {
                    if s.is_null(i) || f.is_null(i) {
                        results.push(None);
                    } else {
                        let (months, days, nanoseconds) =
                            IntervalMonthDayNanoType::to_parts(s.value(i));
                        let factor = f.value(i);

                        let months = (months as f64 * factor) as i32;
                        let days = (days as f64 * factor) as i32;
                        let nanos = (nanoseconds as f64 * factor) as i64;

                        results.push(Some(IntervalMonthDayNanoType::make_value(
                            months, days, nanos,
                        )));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::IntervalMonthDayNanoArray::from(results),
                )))
            }
            (ColumnarValue::Array(s), ColumnarValue::Scalar(f)) => {
                let s = s
                    .as_any()
                    .downcast_ref::<IntervalMonthDayNanoArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "Expected IntervalMonthDayNanoArray".to_string(),
                        )
                    })?;
                let factor = match f {
                    ScalarValue::Float64(Some(v)) => *v,
                    ScalarValue::Int64(Some(v)) => *v as f64,
                    ScalarValue::Decimal128(Some(v), _, scale) => {
                        *v as f64 / 10f64.powi(*scale as i32)
                    }
                    _ => {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "Expected numeric factor".to_string(),
                        ))
                    }
                };

                let mut results = Vec::with_capacity(s.len());
                for i in 0..s.len() {
                    if s.is_null(i) {
                        results.push(None);
                    } else {
                        let (months, days, nanoseconds) =
                            IntervalMonthDayNanoType::to_parts(s.value(i));
                        let months = (months as f64 * factor) as i32;
                        let days = (days as f64 * factor) as i32;
                        let nanos = (nanoseconds as f64 * factor) as i64;
                        results.push(Some(IntervalMonthDayNanoType::make_value(
                            months, days, nanos,
                        )));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::IntervalMonthDayNanoArray::from(results),
                )))
            }
            (ColumnarValue::Scalar(s), ColumnarValue::Array(f)) => {
                let interval_val = match s {
                    ScalarValue::IntervalMonthDayNano(Some(v)) => *v,
                    _ => {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "Expected interval".to_string(),
                        ))
                    }
                };
                let (months_in, days_in, nanos_in) =
                    IntervalMonthDayNanoType::to_parts(interval_val);

                let f = f.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "Expected Float64Array".to_string(),
                    )
                })?;

                let mut results = Vec::with_capacity(f.len());
                for i in 0..f.len() {
                    if f.is_null(i) {
                        results.push(None);
                    } else {
                        let factor = f.value(i);
                        let months = (months_in as f64 * factor) as i32;
                        let days = (days_in as f64 * factor) as i32;
                        let nanos = (nanos_in as f64 * factor) as i64;
                        results.push(Some(IntervalMonthDayNanoType::make_value(
                            months, days, nanos,
                        )));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::IntervalMonthDayNanoArray::from(results),
                )))
            }
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct DivideIntervalFunc {
    signature: Signature,
}

impl DivideIntervalFunc {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),
                    DataType::Float64,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for DivideIntervalFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "divide_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Interval(
            datafusion::arrow::datatypes::IntervalUnit::MonthDayNano,
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let args = args.args;
        match (&args[0], &args[1]) {
            (ColumnarValue::Scalar(s), ColumnarValue::Scalar(f)) => {
                let interval = match s {
                    ScalarValue::IntervalMonthDayNano(Some(v)) => *v,
                    _ => {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "Expected interval".to_string(),
                        ))
                    }
                };
                let factor = match f {
                    ScalarValue::Float64(Some(v)) => *v,
                    ScalarValue::Int64(Some(v)) => *v as f64,
                    ScalarValue::Decimal128(Some(v), _, scale) => {
                        *v as f64 / 10f64.powi(*scale as i32)
                    }
                    _ => {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "Expected numeric factor".to_string(),
                        ))
                    }
                };

                if factor == 0.0 {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "Division by zero in divide_interval".to_string(),
                    ));
                }

                let (months, days, nanoseconds) = IntervalMonthDayNanoType::to_parts(interval);
                let months = (months as f64 / factor) as i32;
                let days = (days as f64 / factor) as i32;
                let nanos = (nanoseconds as f64 / factor) as i64;

                Ok(ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(
                    Some(IntervalMonthDayNanoType::make_value(months, days, nanos)),
                )))
            }
            (ColumnarValue::Array(s), ColumnarValue::Array(f)) => {
                let s = s
                    .as_any()
                    .downcast_ref::<IntervalMonthDayNanoArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "Expected IntervalMonthDayNanoArray".to_string(),
                        )
                    })?;
                let f = f.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "Expected Float64Array".to_string(),
                    )
                })?;

                if s.len() != f.len() {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "Mismatched array lengths in divide_interval".to_string(),
                    ));
                }

                let mut results = Vec::with_capacity(s.len());
                for i in 0..s.len() {
                    if s.is_null(i) || f.is_null(i) {
                        results.push(None);
                    } else {
                        let (months, days, nanoseconds) =
                            IntervalMonthDayNanoType::to_parts(s.value(i));
                        let factor = f.value(i);

                        if factor == 0.0 {
                            return Err(datafusion::error::DataFusionError::Execution(
                                "Division by zero in divide_interval".to_string(),
                            ));
                        }

                        let months = (months as f64 / factor) as i32;
                        let days = (days as f64 / factor) as i32;
                        let nanos = (nanoseconds as f64 / factor) as i64;

                        results.push(Some(IntervalMonthDayNanoType::make_value(
                            months, days, nanos,
                        )));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::IntervalMonthDayNanoArray::from(results),
                )))
            }
            (ColumnarValue::Array(s), ColumnarValue::Scalar(f)) => {
                let s = s
                    .as_any()
                    .downcast_ref::<IntervalMonthDayNanoArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "Expected IntervalMonthDayNanoArray".to_string(),
                        )
                    })?;
                let factor = match f {
                    ScalarValue::Float64(Some(v)) => *v,
                    ScalarValue::Int64(Some(v)) => *v as f64,
                    ScalarValue::Decimal128(Some(v), _, scale) => {
                        *v as f64 / 10f64.powi(*scale as i32)
                    }
                    _ => {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "Expected numeric factor".to_string(),
                        ))
                    }
                };
                if factor == 0.0 {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "Division by zero in divide_interval".to_string(),
                    ));
                }

                let mut results = Vec::with_capacity(s.len());
                for i in 0..s.len() {
                    if s.is_null(i) {
                        results.push(None);
                    } else {
                        let (months, days, nanoseconds) =
                            IntervalMonthDayNanoType::to_parts(s.value(i));
                        let months = (months as f64 / factor) as i32;
                        let days = (days as f64 / factor) as i32;
                        let nanos = (nanoseconds as f64 / factor) as i64;
                        results.push(Some(IntervalMonthDayNanoType::make_value(
                            months, days, nanos,
                        )));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::IntervalMonthDayNanoArray::from(results),
                )))
            }
            (ColumnarValue::Scalar(s), ColumnarValue::Array(f)) => {
                let interval_val = match s {
                    ScalarValue::IntervalMonthDayNano(Some(v)) => *v,
                    _ => {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "Expected interval".to_string(),
                        ))
                    }
                };
                let (months_in, days_in, nanos_in) =
                    IntervalMonthDayNanoType::to_parts(interval_val);

                let f = f.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "Expected Float64Array".to_string(),
                    )
                })?;

                let mut results = Vec::with_capacity(f.len());
                for i in 0..f.len() {
                    if f.is_null(i) {
                        results.push(None);
                    } else {
                        let factor = f.value(i);
                        if factor == 0.0 {
                            return Err(datafusion::error::DataFusionError::Execution(
                                "Division by zero in divide_interval".to_string(),
                            ));
                        }
                        let months = (months_in as f64 / factor) as i32;
                        let days = (days_in as f64 / factor) as i32;
                        let nanos = (nanos_in as f64 / factor) as i64;
                        results.push(Some(IntervalMonthDayNanoType::make_value(
                            months, days, nanos,
                        )));
                    }
                }
                Ok(ColumnarValue::Array(Arc::new(
                    datafusion::arrow::array::IntervalMonthDayNanoArray::from(results),
                )))
            }
        }
    }
}
