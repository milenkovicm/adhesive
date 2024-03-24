use regex::Regex;

use crate::JvmFunctionError;

const BASE_PACKAGE: &str = "com.github.milenkovicm.generated";

fn generate_package_name() -> String {
    let charset = "abcdefghijklmnopqrstuvwxyz";

    format!("{}.p{}", BASE_PACKAGE, random_string::generate(6, charset))
}

fn find_class_name(code: &str) -> Option<String> {
    let re = Regex::new(r"class\s+(?<name>\w+)").unwrap();

    let capture = re.captures(code)?;

    Some(capture["name"].to_string())
}

/// creates new FQN for given code, and
/// updates java code adding package definition
/// returns (code, FQN)
pub(crate) fn update_java_code(java_code: &str) -> crate::jvm::Result<(String, String)> {
    let generated_class_name = find_class_name(java_code).ok_or(
        JvmFunctionError::JavaCodeError("Can't find class name".into()),
    )?;

    let generated_package_name = generate_package_name();

    let fqn = format!("{}.{}", generated_package_name, generated_class_name);
    Ok((
        format!("package {};\n{}", generated_package_name, java_code),
        fqn,
    ))
}

#[cfg(test)]
mod test {
    use crate::util::find_class_name;

    #[test]
    fn should_find_class_name() {
        let code = r#"
        import java.util.List;
        public class  ClassName1 {
            // this is a comment 
        }
        "#;
        assert_eq!("ClassName1", find_class_name(code).unwrap())
    }
}
