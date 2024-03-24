use std::sync::Arc;

use arrow::{array::ArrayRef, datatypes::DataType};
use datafusion::{
    common::exec_err,
    execution::{
        config::SessionConfig,
        context::{FunctionFactory, RegisterFunction},
    },
    logical_expr::{
        ColumnarValue, CreateFunction, DefinitionStatement, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility,
    },
};

use crate::{
    jvm::{JvmFunction, JvmFunctionFactory},
    JvmFunctionError,
};
use datafusion::error::{DataFusionError, Result};

#[async_trait::async_trait]
impl FunctionFactory for JvmFunctionFactory {
    async fn create(
        &self,
        _state: &SessionConfig,
        statement: CreateFunction,
    ) -> Result<RegisterFunction> {
        let return_type = statement.return_type.expect("return type expected");
        let method_name = Self::return_type_to_method_name(&return_type)?;

        let language = statement
            .params
            .language
            .map(|i| i.value.to_lowercase())
            .unwrap_or("java".to_string());

        let (jvm_function, function_definition) = match (&statement.params.as_, language.as_str()) {
            (Some(DefinitionStatement::SingleQuotedDef(java_code)), "java") => (
                self.compile_create_function(java_code, &method_name)?,
                FunctionDefinition::Java {
                    class_definition: java_code.to_owned(),
                },
            ),
            (Some(DefinitionStatement::SingleQuotedDef(class_name)), "class") => (
                self.create_function(class_name, &method_name)?,
                FunctionDefinition::Fqn {
                    fqn: class_name.to_owned(),
                },
            ),

            // Double dollar def does not work.
            // It was intended to use for java code definition
            // Some(DefinitionStatement::DoubleDollarDef(java_code)) => {
            //     self.create_function(&class_name, &method_name)?
            // }
            _ => exec_err!("class name or class definition should be provided")?,
        };

        let argument_types = statement
            .args
            .map(|args| {
                args.into_iter()
                    .map(|a| a.data_type)
                    .collect::<Vec<DataType>>()
            })
            .unwrap_or_default();

        let f = JvmFunctionWrapper {
            name: statement.name,
            argument_types: argument_types.clone(),
            signature: Signature::exact(argument_types, Volatility::Volatile),
            function_definition,
            return_type,
            inner: jvm_function,
        };

        Ok(RegisterFunction::Scalar(Arc::new(ScalarUDF::from(f))))
    }
}

impl JvmFunctionFactory {
    fn return_type_to_method_name(return_type: &DataType) -> Result<String> {
        let method_name = match return_type {
            DataType::Int64 => "computeBigInt",
            _ => exec_err!("type not supported (to be added)")?,
        };

        Ok(method_name.into())
    }
}

#[derive(Debug)]
struct JvmFunctionWrapper {
    name: String,
    argument_types: Vec<DataType>,
    signature: Signature,
    return_type: DataType,
    function_definition: FunctionDefinition,
    inner: JvmFunction,
}

impl ScalarUDFImpl for JvmFunctionWrapper {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[arrow::datatypes::DataType],
    ) -> Result<arrow::datatypes::DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke(
        &self,
        args: &[datafusion::logical_expr::ColumnarValue],
    ) -> Result<datafusion::logical_expr::ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        let array = JvmFunction::create_arrow_data(&self.argument_types, &arrays)?;

        let result = self.inner.invoke_java(array)?;

        Ok(ColumnarValue::from(result as ArrayRef))
    }
}

impl From<JvmFunctionError> for DataFusionError {
    fn from(error: JvmFunctionError) -> Self {
        DataFusionError::Execution(error.to_string())
    }
}

/// Captures how java function has been defined

// To be used later for function serialization
#[derive(Debug)]
enum FunctionDefinition {
    /// Fully qualified class name
    Fqn { fqn: String },
    /// Class definition
    Java { class_definition: String },
    /// Compiled class definition (byte_code)
    Class { byte_code: Vec<u8>, fqn: String },
}
