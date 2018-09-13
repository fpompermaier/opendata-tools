package it.okkam.opendata.geonames.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class RowEnlarger implements MapFunction<Row, Row> {

  private static final long serialVersionUID = 1L;
  private final Row reuse;

  public RowEnlarger(int rowSize) {
    this.reuse = new Row(rowSize);
  }

  @Override
  public Row map(Row row) throws Exception {
    for (int i = 0; i < row.getArity(); i++) {
      reuse.setField(i, row.getField(i));
    }
    return reuse;
  }

}
