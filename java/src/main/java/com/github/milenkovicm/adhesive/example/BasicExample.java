package com.github.milenkovicm.adhesive.example;

import com.github.milenkovicm.adhesive.Adhesive;
import org.apache.arrow.vector.table.Row;

public class BasicExample extends Adhesive {

  /**
   * Calculates computation result
   *
   * @param row
   * @return
   */
  @Override
  protected Long compute(Row row) {
    return row.getBigInt(0) + row.getBigInt(1);
  }
}
