package it.okkam.flink.csv;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class CsvLine2Row extends RichFlatMapFunction<String, Row> {

  private static final long serialVersionUID = 1L;

  private transient Row reuse;
  private transient CSVFormat csvFormat;

  private final String headerStr;
  private final String[] header;
  private final char fieldDelimiter;
  private final Character withQuote;
  private final boolean skipInvalidRows;

  public static final String INVALID_LINES_ACCUM = "invalidLines";
  private final IntCounter invalidLines = new IntCounter();

  /**
   * Constructor that allows to configure the field delimiter.
   * 
   * @param headerStr the header string (separated by fieldDelimiter)
   * @param skipInvalidRows true to skip invalid rows and just track the error (via accumulator)
   * @param fieldDelimiter the delimiter char
   * @param withQuote the String quote char (could be null)
   */
  public CsvLine2Row(String headerStr, boolean skipInvalidRows, char fieldDelimiter,
      Character withQuote) {
    this.headerStr = headerStr;
    this.header = headerStr.split(Pattern.quote(fieldDelimiter + ""));
    this.fieldDelimiter = fieldDelimiter;
    this.withQuote = withQuote;
    this.skipInvalidRows = skipInvalidRows;
  }

  @Override
  public void open(
      @SuppressWarnings("unused") org.apache.flink.configuration.Configuration parameters)
      throws Exception {
    csvFormat = CSVFormat.newFormat(fieldDelimiter)//
        .withQuote(withQuote)//
        .withQuoteMode(QuoteMode.MINIMAL)//
        .withNullString("").//
        withHeader(header).//
        withIgnoreSurroundingSpaces(true);
    this.reuse = new Row(header.length);
    getRuntimeContext().addAccumulator(INVALID_LINES_ACCUM, this.invalidLines);
  }

  @Override
  public void flatMap(String line, Collector<Row> out) throws Exception {
    // skip header
    if (line.equals(headerStr)) {
      return;
    }
    List<CSVRecord> rowList;
    try {
      rowList = CSVParser.parse(line, csvFormat).getRecords();
      for (CSVRecord row : rowList) {
        if (row.size() != header.length) {
          throw new IOException("Invalid line lenght (current: " + row.size() + ", expected: "
              + header.length + ") for line " + line);
        }
        for (int i = 0; i < header.length; i++) {
          reuse.setField(i, row.get(header[i]));
        }
        out.collect(reuse);
      }
    } catch (IOException ex) {
      if (skipInvalidRows) {
        invalidLines.add(1);
      } else {
        throw new IOException("Invalid line: " + line, ex);
      }
    }
  }


}
