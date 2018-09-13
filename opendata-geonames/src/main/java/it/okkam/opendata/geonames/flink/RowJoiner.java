package it.okkam.opendata.geonames.flink;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.types.Row;

public class RowJoiner implements JoinFunction<Row, Row, Row> {

  private static final long serialVersionUID = 1L;
  private final Row reuse;

  public RowJoiner(int rowSize) {
    this.reuse = new Row(rowSize);
  }

  @Override
  public Row join(Row leftRow, Row rightRow) throws Exception {
    int i = 0;
    for (; i < leftRow.getArity(); i++) {
      reuse.setField(i, leftRow.getField(i));
    }
    for (int j = 0; j < rightRow.getArity(); j++) {
      reuse.setField(i++, rightRow.getField(j));
    }
    return reuse;
  }
}
