package com.github.milenkovicm.adhesive;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import javax.tools.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compiles given strings to java instances
 *
 * @link "https://www.baeldung.com/java-string-compile-execute-code"
 */

// TODO: add class unloading at some point

public class Compiler {

  public static Compiler INSTANCE = new Compiler();

  static final Logger LOGGER = LoggerFactory.getLogger(Compiler.class);
  static final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

  static final InMemoryFileManager manager =
      new InMemoryFileManager(compiler.getStandardFileManager(null, null, null));

  public Adhesive compile(String qualifiedClassName, String sourceCode)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    LOGGER.debug("Compiling class of: `{}`: \n ```java\n{}\n```", qualifiedClassName, sourceCode);

    DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();

    List<JavaFileObject> sourceFiles =
        Collections.singletonList(new JavaSourceFromString(qualifiedClassName, sourceCode));

    JavaCompiler.CompilationTask task =
        compiler.getTask(null, manager, diagnostics, null, null, sourceFiles);

    if (!task.call()) {
      diagnostics.getDiagnostics().forEach(d -> LOGGER.error(String.valueOf(d)));

      // should we return null or throw exception here ?
      // return null;
      throw new InstantiationException("Can't compile function");
    } else {
      ClassLoader classLoader = manager.getClassLoader(null);
      Class<?> clazz = classLoader.loadClass(qualifiedClassName);
      Adhesive instanceOfClass = (Adhesive) clazz.newInstance();

      return instanceOfClass;
    }
  }
}

class JavaSourceFromString extends SimpleJavaFileObject {

  private String sourceCode;

  public JavaSourceFromString(String name, String sourceCode) {
    super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
    this.sourceCode = requireNonNull(sourceCode, "sourceCode must not be null");
  }

  @Override
  public CharSequence getCharContent(boolean ignoreEncodingErrors) {
    return sourceCode;
  }
}

class JavaClassAsBytes extends SimpleJavaFileObject {

  protected ByteArrayOutputStream bos = new ByteArrayOutputStream();

  public JavaClassAsBytes(String name, Kind kind) {
    super(URI.create("string:///" + name.replace('.', '/') + kind.extension), kind);
  }

  public byte[] getBytes() {
    return bos.toByteArray();
  }

  @Override
  public OutputStream openOutputStream() {
    return bos;
  }
}

class InMemoryFileManager extends ForwardingJavaFileManager<JavaFileManager> {

  private final Map<String, JavaClassAsBytes> compiledClasses;
  private final ClassLoader loader;

  /**
   * Creates a new instance of ForwardingJavaFileManager.
   *
   * @param fileManager delegate to this file manager
   */
  public InMemoryFileManager(JavaFileManager fileManager) {
    super(fileManager);
    this.compiledClasses = new Hashtable<>();
    this.loader = new InMemoryClassLoader(this.getClass().getClassLoader(), this);
  }

  @Override
  public ClassLoader getClassLoader(Location location) {
    return loader;
  }

  @Override
  public JavaFileObject getJavaFileForOutput(
      Location location, String className, JavaFileObject.Kind kind, FileObject sibling) {

    JavaClassAsBytes classAsBytes = new JavaClassAsBytes(className, kind);
    compiledClasses.put(className, classAsBytes);

    return classAsBytes;
  }

  public Map<String, JavaClassAsBytes> getBytesMap() {
    return compiledClasses;
  }
}

class InMemoryClassLoader extends ClassLoader {

  private InMemoryFileManager manager;

  public InMemoryClassLoader(ClassLoader parent, InMemoryFileManager manager) {
    super(parent);
    this.manager = requireNonNull(manager, "manager must not be null");
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {

    Map<String, JavaClassAsBytes> compiledClasses = manager.getBytesMap();

    if (compiledClasses.containsKey(name)) {
      byte[] bytes = compiledClasses.get(name).getBytes();
      return defineClass(name, bytes, 0, bytes.length);
    } else {
      throw new ClassNotFoundException();
    }
  }
}
