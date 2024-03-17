package com.github.milenkovicm.adhesive;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class CompilerTest {

  static final String sourceCode =
      "package com.github.milenkovicm.newclass;\n"
          // + "import com.github.milenkovicm.arrowsee.Adhesive;\n"
          // + "import org.apache.arrow.vector.table.Row;\n"
          + "public class NewClass extends com.github.milenkovicm.adhesive.Adhesive{\n"
          + " \n"
          + "@Override\n"
          + "    public Long compute(org.apache.arrow.vector.table.Row row) {\n"
          + "        System.out.println(\"Hello World!\");\n"
          + "        return null; \n"
          + "    }\n"
          + "}\n";

  @Test
  public void basicCompileTest() throws Exception {

    var compiler = new Compiler();

    var instance = compiler.compile("com.github.milenkovicm.newclass.NewClass", sourceCode);

    assertNotNull(instance);
    instance.compute(null);
  }

  @Test()
  public void basicFailCompileTest() throws Exception {

    var compiler = new Compiler();

    var exception =
        assertThrows(
            InstantiationException.class,
            // we break FQN to fail compilation
            () -> compiler.compile("com.github.milenkovicm.newclass.NewClass1", sourceCode));

    assertEquals("Can't compile function", exception.getMessage());
  }
}
