// TODO: remove this later
#![allow(dead_code)]

pub use crate::jvm::JvmFunctionFactory;

use thiserror::Error;
mod fusion;
mod jvm;
mod util;

#[derive(Error, Debug)]
pub enum JvmFunctionError {
    #[error("Error starting JVM: {0}")]
    StartJvmError(#[from] jni::errors::StartJvmError),
    #[error("JVM error: {0}")]
    JvmFunctionError(#[from] jni::JvmError),
    #[error("JNI error: {0}")]
    JniErrors(#[from] jni::errors::Error),
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error("JVM call throw exception: {0}")]
    JvmException(String),
    #[error("Java code error: {0}")]
    JavaCodeError(String),
}
