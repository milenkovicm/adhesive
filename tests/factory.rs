#[cfg(test)]
mod test {
    use std::sync::Arc;

    use adhesive::JvmFunctionFactory;
    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use datafusion::{assert_batches_eq, execution::context::SessionContext};

    const JAR_PATH: &str = "java/target/adhesive-jar-with-dependencies.jar";

    #[tokio::test]
    async fn should_invoke_java() -> datafusion::error::Result<()> {
        let ctx = SessionContext::new()
            // register custom function factory
            .with_function_factory(Arc::new(JvmFunctionFactory::new_with_jar(JAR_PATH)?));

        let a: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)])?;

        ctx.register_batch("t", batch)?;

        let sql = r#"
        CREATE FUNCTION f1(BIGINT, BIGINT)
        RETURNS BIGINT
        LANGUAGE JAVA
        AS '
        public class NewClass extends com.github.milenkovicm.adhesive.Adhesive {
            @Override
            public Long compute(org.apache.arrow.vector.table.Row row) {
                return row.getBigInt(0) * row.getBigInt(1); 
            }
        }
        '
        "#;

        ctx.sql(sql).await?.show().await?;

        let result = ctx.sql("select f1(a,b) from t").await?.collect().await?;

        let expected = vec![
            "+-------------+",
            "| f1(t.a,t.b) |",
            "+-------------+",
            "| 10          |",
            "| 40          |",
            "| 90          |",
            "| 160         |",
            "+-------------+",
        ];
        assert_batches_eq!(expected, &result);

        // note change in language
        let sql = r#"
        CREATE FUNCTION f2(BIGINT, BIGINT)
        RETURNS BIGINT
        LANGUAGE CLASS
        AS "com.github.milenkovicm.adhesive.example.BasicExample"
        "#;

        ctx.sql(sql).await?.show().await?;

        let result = ctx.sql("select f2(a,b) from t").await?.collect().await?;

        let expected = vec![
            "+-------------+",
            "| f2(t.a,t.b) |",
            "+-------------+",
            "| 11          |",
            "| 22          |",
            "| 33          |",
            "| 44          |",
            "+-------------+",
        ];
        assert_batches_eq!(expected, &result);

        Ok(())
    }
}
