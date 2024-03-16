package com.github.milenkovicm.adhesive.example;

import com.github.milenkovicm.adhesive.Adhesive;
import org.apache.arrow.vector.table.Row;

/** Test of exception handling */
public class FreaksOutExample extends Adhesive {

  @Override
  protected <T> T compute(Row row) {
    throw new RuntimeException("Its ok to freak out sometimes");
  }
}
