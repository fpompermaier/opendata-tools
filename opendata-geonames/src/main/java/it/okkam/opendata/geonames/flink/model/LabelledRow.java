package it.okkam.opendata.geonames.flink.model;

import it.okkam.opendata.geonames.flink.FlinkUtils;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.types.Row;

public class LabelledRow extends Tuple2<String, Row> {

  private static final long serialVersionUID = 1L;

  public LabelledRow() {
  }
  
  public LabelledRow(String label, Row row) {
    this.f0 = label;
    this.f1 = row;
  }

  public String getLabel() {
    return this.f0;
  }

  public void setLabel(String label) {
    this.f0 = label;
  }

  public Row getRow() {
    return this.f1;
  }

  public void setRow(Row row) {
    this.f1 = row;
  }

  public static RowTypeInfo getRowTypeInfo(String[] fieldNames) {
    return new RowTypeInfo(FlinkUtils.getDefaultFlinkFieldTypes(fieldNames));
  }

  public static TupleTypeInfo<LabelledRow> getTypeInfo(String[] fieldNames) {
    return new TupleTypeInfo<>(LabelledRow.class, BasicTypeInfo.STRING_TYPE_INFO, getRowTypeInfo(fieldNames));
  }

}
