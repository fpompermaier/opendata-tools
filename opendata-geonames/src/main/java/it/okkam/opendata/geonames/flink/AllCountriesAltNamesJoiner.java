package it.okkam.opendata.geonames.flink;

import java.util.Map;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

@ForwardedFieldsFirst("0")
public class AllCountriesAltNamesJoiner
    implements JoinFunction<Tuple2<String, Row>, Row, Tuple2<String, Row>> {

  private static final long serialVersionUID = 1L;
  private String[] leftFields;
  private String[] rightFields;
  private final Map<String, Integer> leftFieldPos;
  private final Map<String, Integer> rightFieldPos;
  private final Map<String, Integer> outFieldsPos;
  private final Tuple2<String, Row> reuse = new Tuple2<>();

  public AllCountriesAltNamesJoiner(String[] leftFields, String[] rightFields, Map<String,Integer> outFieldsPos,
      Map<String, Integer> leftFieldPos, Map<String, Integer> rightFieldPos) {
    this.leftFields = leftFields;
    this.rightFields = rightFields;
    this.leftFieldPos = leftFieldPos;
    this.rightFieldPos = rightFieldPos;
    this.outFieldsPos = outFieldsPos;
    this.reuse.f1 = new Row(outFieldsPos.size());
  }

  @Override
  public Tuple2<String, Row> join(Tuple2<String, Row> locRow, Row altNamesRow) throws Exception {
    reuse.f0 = locRow.f0;
    for (String fieldName : leftFields) {
      final Integer outFieldPos = outFieldsPos.get(fieldName);
      reuse.f1.setField(outFieldPos, locRow.f1.getField(leftFieldPos.get(fieldName)));

    }
    for (String fieldName : rightFields) {
      final Integer outFieldPos = outFieldsPos.get(fieldName);
      reuse.f1.setField(outFieldPos, altNamesRow.getField(rightFieldPos.get(fieldName)));

    }
    return reuse;

  }
}
