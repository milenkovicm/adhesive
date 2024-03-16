package com.github.milenkovicm.adhesive;

import java.util.Iterator;
import java.util.function.BiFunction;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.table.Row;
import org.apache.arrow.vector.table.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class which links rust with java.
 *
 * <p>Caller should decide which method to call base on return type they expect.
 *
 * <p>Implementor should provide default constructor.
 *
 * @link "https://arrow.apache.org/docs/java/cdata.html"
 * @link "https://arrow.apache.org/docs/java/table.html"
 * @link "https://docs.rs/arrow/latest/arrow/ffi/index.html"
 */
public abstract class Adhesive {

  static final Logger logger = LoggerFactory.getLogger(Adhesive.class);

  static final BufferAllocator allocator = new RootAllocator();

  /**
   * Computation method
   *
   * <p>Implementors should not access fields by name.
   *
   * @param row
   * @return value or null unless used by computeNonNull methods
   * @param <T> result type
   */
  protected abstract <T> T compute(Row row);

  private <T extends FixedWidthVector & FieldVector> void computeInternal(
      long addressInputSchema,
      long addressInputArray,
      long addressOutputSchema,
      long addressOutputArray,
      T resultVector,
      BiFunction<Integer, Row, Void> addToResult) {

    try (ArrowArray inputArray = ArrowArray.wrap(addressInputArray);
        ArrowSchema inputSchema = ArrowSchema.wrap(addressInputSchema);
        ArrowArray outputArray = ArrowArray.wrap(addressOutputArray);
        ArrowSchema outputSchema = ArrowSchema.wrap(addressOutputSchema)) {

      logger.debug("java invoked ...");

      var vector = Data.importVector(allocator, inputArray, inputSchema, null);
      var table = new Table(vector.getChildrenFromFields());
      var resultCount = table.getRowCount();

      logger.debug("java invoked ... input vector size: {}", resultCount);

      resultVector.allocateNew((int) resultCount);

      var offset = 0;
      for (Iterator<Row> it = table.iterator(); it.hasNext(); offset++) {
        addToResult.apply(offset, it.next());
      }

      resultVector.setValueCount(offset);
      Data.exportVector(allocator, resultVector, null, outputArray, outputSchema);

      logger.debug("java invoked ... DONE");
    }
  }

  public void computeBigInt(
      long addressInputSchema,
      long addressInputArray,
      long addressOutputSchema,
      long addressOutputArray) {

    var result = new BigIntVector("result", allocator);

    this.computeInternal(
        addressInputSchema,
        addressInputArray,
        addressOutputSchema,
        addressOutputArray,
        result,
        (index, row) -> {
          var r = this.<Long>compute(row);
          if (r != null) {
            result.set(index, r);
          } else {
            result.setNull(index);
          }

          return null;
        });
  }

  public void computeNonNullBigInt(
      long addressInputSchema,
      long addressInputArray,
      long addressOutputSchema,
      long addressOutputArray) {

    var result = new BigIntVector("result", allocator);

    this.computeInternal(
        addressInputSchema,
        addressInputArray,
        addressOutputSchema,
        addressOutputArray,
        result,
        (index, row) -> {
          result.set(index, this.<Long>compute(row));

          return null;
        });
  }

  public void computeInt(
      long addressInputSchema,
      long addressInputArray,
      long addressOutputSchema,
      long addressOutputArray) {

    var result = new IntVector("result", allocator);

    this.computeInternal(
        addressInputSchema,
        addressInputArray,
        addressOutputSchema,
        addressOutputArray,
        result,
        (index, row) -> {
          var r = this.<Integer>compute(row);
          if (r != null) {
            result.set(index, r);
          } else {
            result.setNull(index);
          }
          return null;
        });
  }

  public void computeFloat(
      long addressInputSchema,
      long addressInputArray,
      long addressOutputSchema,
      long addressOutputArray) {

    var result = new Float4Vector("result", allocator);

    this.computeInternal(
        addressInputSchema,
        addressInputArray,
        addressOutputSchema,
        addressOutputArray,
        result,
        (index, row) -> {
          var r = this.<Float>compute(row);
          if (r != null) {
            result.set(index, r);
          } else {
            result.setNull(index);
          }
          return null;
        });
  }

  public void computeDouble(
      long addressInputSchema,
      long addressInputArray,
      long addressOutputSchema,
      long addressOutputArray) {

    var result = new Float8Vector("result", allocator);

    this.computeInternal(
        addressInputSchema,
        addressInputArray,
        addressOutputSchema,
        addressOutputArray,
        result,
        (index, row) -> {
          var r = this.<Double>compute(row);
          if (r != null) {
            result.set(index, r);
          } else {
            result.setNull(index);
          }
          return null;
        });
  }
}
