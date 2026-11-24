package io.frostmq.demo;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CsvFormatTest {
  @Test
  void quotesFieldsAndNeutralizesSpreadsheetFormulas() {
    assertEquals("\"warehouse-a\"", CsvFormat.cell("warehouse-a"));
    assertEquals("\"'=1+1\"", CsvFormat.cell("=1+1"));
    assertEquals("\"'+1\"", CsvFormat.cell("+1"));
    assertEquals("\"'-1\"", CsvFormat.cell("-1"));
    assertEquals("\"'@SUM(A1)\"", CsvFormat.cell("@SUM(A1)"));
    assertEquals("\"'\tcmd\"", CsvFormat.cell("\tcmd"));
    assertEquals("\"'\r=1\"", CsvFormat.cell("\r=1"));
    assertEquals("\"'\n=1\"", CsvFormat.cell("\n=1"));
    assertEquals("\"a\"\"b\"", CsvFormat.cell("a\"b"));
    assertEquals("\"\"", CsvFormat.cell(""));
    assertEquals("\"\"", CsvFormat.cell(null));
  }
}
