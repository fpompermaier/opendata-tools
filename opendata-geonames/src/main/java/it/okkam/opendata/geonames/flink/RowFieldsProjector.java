package it.okkam.opendata.geonames.flink;

import it.okkam.opendata.geonames.GeoNamesUtils;

import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class RowFieldsProjector implements MapFunction<Row, Row> {
  private final String[] inFields;
  private static final long serialVersionUID = 1L;
  private final Map<String, Integer> fieldsMap;
  private final Row reuse;

  public RowFieldsProjector(String[] projectedFields, String[] inFields) {
    this.inFields = inFields;
    this.fieldsMap = GeoNamesUtils.getFieldPosMap(projectedFields);
    this.reuse = new Row(projectedFields.length);
  }

  @Override
  public Row map(Row row) throws Exception {
    for (int i = 0; i < row.getArity(); i++) {
      if (fieldsMap.containsKey(inFields[i])) {
        reuse.setField(fieldsMap.get(inFields[i]), row.getField(i));
      }
    }
    return reuse;
  }
}
