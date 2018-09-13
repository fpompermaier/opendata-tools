package it.okkam.opendata.geonames.flink;

import it.okkam.opendata.geonames.flink.model.LabelledRow;

import java.util.Map;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.types.Row;

public class LabelledRowJoiner implements JoinFunction<LabelledRow, Row, LabelledRow> {

  private static final long serialVersionUID = 1L;
  private String[] leftFields;
  private String[] rightFields;
  private final Map<String, Integer> leftFieldPos;
  private final Map<String, Integer> rightFieldPos;
  private final Map<String, Integer> outFieldsPos;
  private final LabelledRow reuse = new LabelledRow();

  public LabelledRowJoiner(String[] leftFields, String[] rightFields,
      Map<String, Integer> outFieldsPos, Map<String, Integer> leftFieldPos,
      Map<String, Integer> rightFieldPos) {
    this.leftFields = leftFields;
    this.rightFields = rightFields;
    this.leftFieldPos = leftFieldPos;
    this.rightFieldPos = rightFieldPos;
    this.outFieldsPos = outFieldsPos;
    this.reuse.setRow(new Row(outFieldsPos.size()));
  }

  @Override
  public LabelledRow join(LabelledRow labelledRow, Row rowToJoin) throws Exception {
    reuse.setLabel(labelledRow.getLabel());
    for (String fieldName : leftFields) {
      final Integer outFieldPos = outFieldsPos.get(fieldName);
      reuse.getRow().setField(outFieldPos, labelledRow.getRow().getField(leftFieldPos.get(fieldName)));

    }
    for (String fieldName : rightFields) {
      final Integer outFieldPos = outFieldsPos.get(fieldName);
      reuse.getRow().setField(outFieldPos, rowToJoin.getField(rightFieldPos.get(fieldName)));

    }
    return reuse;
  }
}
