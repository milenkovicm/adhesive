# Adhesive - Datafusion Integration Nobody Asked For

Very opinionated datafusion user defined functions written in java.

It has been implemented to demonstrate DataFusion `FunctionFactory` functionality ([arrow-datafusion/pull#9333](https://github.com/apache/arrow-datafusion/pull/9333)).

> [!NOTE]
> It has not been envisaged as a actively maintained library.
>
Other project utilizing `FunctionFactory`:

- [Torchfusion, Opinionated Torch Inference on DataFusion](https://github.com/milenkovicm/torchfusion)
- [LightGBM Inference on DataFusion](https://github.com/milenkovicm/lightfusion)

## How To Use

A java user defined function can be defined using `CREATE FUNCTION`:

```sql
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
```

which will be compiled. Or, referencing existing compiled class:

```sql
CREATE FUNCTION f2(BIGINT, BIGINT)
RETURNS BIGINT
LANGUAGE CLASS
AS "com.github.milenkovicm.adhesive.example.BasicExample"
```

Note change of `LANGUAGE`. In both cases classes should extend `com.github.milenkovicm.adhesive.Adhesive` abstract class,
which will do transition between rust and java.

`com.github.milenkovicm.adhesive.example.BasicExample` is defined like:

```java
package com.github.milenkovicm.adhesive.example;

import com.github.milenkovicm.adhesive.Adhesive;
import org.apache.arrow.vector.table.Row;

public class BasicExample extends Adhesive {
  @Override
  protected Long compute(Row row) {
    return row.getBigInt(0) + row.getBigInt(1);
  }
}
```

Defined functions can be invoked in SQL:

```sql
SELECT f2(a,b) FROM t
```

## Setup

```rust
// JAR containing required libraries and additional classes 
const JAR_PATH: &str ="adhesive-1.0-jar-with-dependencies.jar";
let ctx = SessionContext::new()
    .with_function_factory(Arc::new(JvmFunctionFactory::new_with_jar(JAR_PATH)?));
```
