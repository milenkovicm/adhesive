use crate::JvmFunctionError;
use datafusion::arrow::{
    array::{make_array, Array, ArrayData, ArrayDataBuilder, ArrayRef},
    datatypes::{DataType, Field, Fields},
    ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema},
};
use jni::{
    objects::{GlobalRef, JMethodID, JValue},
    signature::ReturnType,
    InitArgs, InitArgsBuilder, JNIEnv, JNIVersion, JavaVM,
};
use std::{
    ptr::{addr_of, addr_of_mut},
    sync::Arc,
};

pub(crate) type Result<T> = std::result::Result<T, JvmFunctionError>;

use once_cell::sync::OnceCell;

// in practice we don't need once cell
// but test complains as they start multiple JvmFunctionFactory
// instances which start multiple JVMs.
// At the moment it looks like there is a
// one JVM per process limitation
static JVM: OnceCell<Arc<JavaVM>> = OnceCell::new();

/// type of base class all function must extend
static BASE_CLASS_TYPE: &str = "com/github/milenkovicm/adhesive/Adhesive";
/// java compiler class implementation
static COMPILER_CLASS_NAME: &str = "com/github/milenkovicm/adhesive/Compiler";
/// compiler method
static COMPILER_METHOD_NAME: &str = "compile";

pub struct JvmFunctionFactory {
    // TODO: we should start VM lazily
    //      when first function is created
    jvm: Arc<JavaVM>,
}

impl JvmFunctionFactory {
    pub fn new_with_jar(jar_path: &str) -> Result<Self> {
        let jvm_args = InitArgsBuilder::new()
            .version(JNIVersion::V8)
            // this guy complains a lot
            // we shut him down for now
            //.option("-Xcheck:jni")
            // TODO: Check if file exist
            .option("--add-opens=java.base/java.nio=ALL-UNNAMED")
            .option(format!("-Djava.class.path={}", jar_path))
            .build()?;

        Self::new_from_args(jvm_args)
    }

    pub fn new_from_args(jvm_args: InitArgs) -> Result<Self> {
        let jvm = JVM.get_or_init(|| Arc::new(JavaVM::new(jvm_args).expect("error to create jvm")));
        Ok(Self { jvm: jvm.clone() })
    }

    pub(crate) fn create_function(
        &self,
        class_name: &str,
        method_name: &str,
    ) -> Result<JvmFunction> {
        log::info!(
            "create function for class: [{}] and method: [{}]",
            class_name,
            method_name
        );
        let mut env = _attach_tread(&self.jvm)?;

        let class_name = class_name.replace('.', "/");
        let clazz = env.find_class(&class_name)?;

        // TODO: check if class is instance of
        // env.is_instance_of(object, class)
        // let subclass = env.get_superclass(&clazz)??;

        let method_id = env.get_method_id(&clazz, method_name, "(JJJJ)V")?;

        // we use default constructor to create this class
        //
        // an alternative was to let user define singleton `INSTANCE`.
        // for now we will allocate new object and cache global reference.
        //
        // alternative was to allocate instance without calling constructor,
        // which we avoided as user might want to use constructor to init class
        // let new_object_instance = env.alloc_object(&clazz)?;

        let new_object_instance = env.new_object(&clazz, "()V", &[])?;

        // The JNI divides object references used by the native code into two
        // categories: local and global references. Local references are valid
        // for the duration of a native method call, and are automatically freed after
        // the native method returns. Global references remain valid until they are explicitly freed.
        //
        // https://docs.oracle.com/javase/8/docs/technotes/guides/jni/spec/design.html

        let object_global_ref = env.new_global_ref(new_object_instance)?;

        Ok(JvmFunction {
            target_method_id: method_id,
            target_object_global_ref: object_global_ref,
            jvm: self.jvm.clone(),
        })
    }

    pub(crate) fn compile_create_function(
        &self,
        java_code: &str,
        method_name: &str,
    ) -> Result<JvmFunction> {
        let mut env = _attach_tread(&self.jvm)?;

        let compiler_clazz = env.find_class(COMPILER_CLASS_NAME)?;
        let compiler_signature =
            format!("(Ljava/lang/String;Ljava/lang/String;)L{BASE_CLASS_TYPE};");

        let compile_method_id =
            env.get_method_id(&compiler_clazz, COMPILER_METHOD_NAME, compiler_signature)?;

        // compiler is a singleton
        // look for static filed called `INSTANCE`
        let compiler_instance = env.get_static_field(
            COMPILER_CLASS_NAME,
            "INSTANCE",
            format!("L{COMPILER_CLASS_NAME};"),
        )?;

        let (java_code, fqn) = crate::util::update_java_code(java_code)?;

        let new_class_name = env.new_string(fqn)?;
        let new_class_definition = env.new_string(java_code)?;
        let new_class_name = JValue::Object(&new_class_name).as_jni();
        let new_class_definition = JValue::Object(&new_class_definition).as_jni();

        unsafe {
            let result = env
                .call_method_unchecked(
                    &compiler_instance.l()?,
                    compile_method_id,
                    ReturnType::Object,
                    &[new_class_name, new_class_definition],
                )?
                .l();

            if has_exception_occurred(&mut env)? {
                Err(JvmFunctionError::JvmException(
                    "no handling for exception messages yet".into(),
                ))
            } else {
                let new_instance = result?;
                let clazz = env.get_object_class(&new_instance)?;
                let target_method_id = env.get_method_id(&clazz, method_name, "(JJJJ)V")?;
                let target_object_global_ref = env.new_global_ref(new_instance)?;

                Ok(JvmFunction {
                    target_method_id,
                    target_object_global_ref,
                    jvm: self.jvm.clone(),
                })
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct JvmFunction {
    target_method_id: JMethodID,

    // TODO: WARN  jni::wrapper::objects::global_ref] Dropping a GlobalRef in a detached thread.
    // Fix your code if this message appears frequently (see the GlobalRef docs).
    //
    // checking current Drop impl for GlobalRef it would do same thing I'll do
    target_object_global_ref: GlobalRef,
    jvm: Arc<JavaVM>,
}

impl JvmFunction {
    pub(crate) fn invoke_java(&self, array: ArrayData) -> Result<Arc<dyn Array>> {
        let mut env = _attach_tread(&self.jvm)?;

        let (input_array, input_schema) = to_ffi(&array)?;

        let ptr_input_array = JValue::from(addr_of!(input_array) as i64).as_jni();
        let ptr_input_schema = JValue::from(addr_of!(input_schema) as i64).as_jni();

        let mut result_schema = FFI_ArrowSchema::empty();
        let mut result_array = FFI_ArrowArray::empty();

        let ptr_return_array = JValue::from(addr_of_mut!(result_array) as i64).as_jni();
        let ptr_return_schema = JValue::from(addr_of_mut!(result_schema) as i64).as_jni();

        unsafe {
            let _result = env.call_method_unchecked(
                &self.target_object_global_ref,
                self.target_method_id,
                ReturnType::Primitive(jni::signature::Primitive::Void),
                &[
                    ptr_input_schema,
                    ptr_input_array,
                    ptr_return_schema,
                    ptr_return_array,
                ],
            );

            if has_exception_occurred(&mut env)? {
                Err(JvmFunctionError::JvmException(
                    "no handling for exception messages yet".into(),
                ))
            } else {
                let result_array = from_ffi(result_array, &result_schema)?;
                Ok(make_array(result_array))
            }
        }
    }

    pub(crate) fn create_arrow_data(dtypes: &[DataType], arrays: &[ArrayRef]) -> Result<ArrayData> {
        // TODO: assert len dtypes and arrays
        let fields = dtypes
            .iter()
            .enumerate()
            .map(|(c, t)| Field::new(format!("_c{}", c), t.clone(), false))
            .collect::<Vec<_>>();

        let fields = Fields::from(fields);

        let array = arrays
            .iter()
            .fold(ArrayDataBuilder::new(DataType::Struct(fields)), |b, a| {
                b.add_child_data(a.to_data())
            });

        Ok(array.build()?)
    }
}

// TODO: it is not clear if we need to clear exception if we
//       propagate it further and `JNIEnv` is closed after
//       propagation
fn has_exception_occurred(env: &mut JNIEnv) -> Result<bool> {
    let result = if env.exception_check()? {
        let _exception = env.exception_occurred()?;
        // good for now
        env.exception_describe()?;
        env.exception_clear()?;

        true
    } else {
        false
    };

    Ok(result)
}

// I'm not sure what's correct approach to attach thread in this case
// should we do it every time or make it daemon ?

// #[inline]
// fn _attach_tread(jvm: &JavaVM) -> std::result::Result<AttachGuard<'_>, jni::errors::Error> {
//     jvm.attach_current_thread()
// }

// Attaching thread as a deamon (or permanently i don't think there is a big difference
// as JVM is going to be shut down when process shuts down)
// if attaching it temporary it makes it harder to debug (as attached thread gets new
// name every time it is registered)
#[inline]
fn _attach_tread(jvm: &JavaVM) -> std::result::Result<JNIEnv, jni::errors::Error> {
    jvm.attach_current_thread_as_daemon()
}
#[cfg(test)]
mod test {

    use super::{JvmFunction, JvmFunctionFactory};
    use datafusion::arrow::{
        array::{ArrayData, ArrayRef, Int64Array},
        datatypes::DataType,
    };
    use std::sync::Arc;

    const JAR_PATH: &str = "java/target/adhesive-jar-with-dependencies.jar";

    #[test]
    fn should_call_basic_example() -> super::Result<()> {
        let factory = JvmFunctionFactory::new_with_jar(JAR_PATH)?;
        let function = factory.create_function(
            "com.github.milenkovicm.adhesive.example.BasicExample",
            "computeBigInt",
        )?;
        let array_data = create_dummy_data()?;
        let result = function.invoke_java(array_data)?;

        println!("{:?}", result);
        assert_eq!(3, result.len());

        Ok(())
    }

    #[test]
    // TODO: do we need to handle error case like when we invoke function
    fn should_fail_to_find_class() -> super::Result<()> {
        let factory = JvmFunctionFactory::new_with_jar(JAR_PATH)?;
        let _function = factory.create_function(
            "com.github.milenkovicm.adhesive.example.FakeExample",
            "computeBigInt",
        );

        assert!(_function.is_err());
        Ok(())
    }

    #[test]
    fn should_handle_exceptions() -> super::Result<()> {
        let factory = JvmFunctionFactory::new_with_jar(JAR_PATH)?;
        let function = factory.create_function(
            "com.github.milenkovicm.adhesive.example.FreaksOutExample",
            "computeBigInt",
        )?;

        let array_data = create_dummy_data()?;
        let result = function.invoke_java(array_data);

        assert!(result.is_err());

        Ok(())
    }

    #[test]
    #[ignore = "there is issue with this test when we run all of them together, !!! FIX !!!"]
    fn should_compile_function() -> super::Result<()> {
        let factory = JvmFunctionFactory::new_with_jar(JAR_PATH)?;
        // package name will be added by the compiler
        // and will be random
        let java_code = r#"
            public class NewClass extends com.github.milenkovicm.adhesive.Adhesive {
                @Override
                public Long compute(org.apache.arrow.vector.table.Row row) {
                    System.out.println("Hello World!");
                    return null; 
                }
            }
            "#;

        let function = factory.compile_create_function(java_code, "computeBigInt")?;
        let array_data = create_dummy_data()?;
        let result = function.invoke_java(array_data)?;

        println!("{:?}", result);
        assert_eq!(3, result.len());

        Ok(())
    }

    fn create_dummy_data() -> super::Result<ArrayData> {
        let array0 = Int64Array::from(vec![Some(100), Some(200), Some(300)]);
        let array1 = Int64Array::from(vec![Some(1001), Some(2002), Some(3003)]);

        let arrays = vec![Arc::new(array0) as ArrayRef, Arc::new(array1)];
        let types = vec![DataType::Int64, DataType::Int64];

        Ok(JvmFunction::create_arrow_data(&types, &arrays)?)
    }
}
