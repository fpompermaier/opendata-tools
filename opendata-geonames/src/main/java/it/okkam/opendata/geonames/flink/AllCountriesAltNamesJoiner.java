package it.okkam.opendata.geonames.flink;

import it.okkam.opendata.geonames.GeoNamesConstants;

import java.util.Map;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

public class AllCountriesAltNamesJoiner
    implements JoinFunction<Tuple2<String, Row>, Row, Tuple2<String, Row>> {
  
  private static final long serialVersionUID = 1L;
  private final String[] altNamesOutFn;
  private final Map<String, Integer> outFnPos;

  public AllCountriesAltNamesJoiner(String[] outFn, String[] altNamesOutFn) {
    this.altNamesOutFn = altNamesOutFn;
    outFnPos = GeoNamesConstants.getFieldPosMap(outFn);
  }

  @Override
  public Tuple2<String, Row> join(Tuple2<String, Row> ret, Row altNamesRow) throws Exception {
    if (altNamesRow != null) {
      for (int i = 1; i < altNamesOutFn.length; i++) {
        ret.f1.setField(outFnPos.get(altNamesOutFn[i]), altNamesRow.getField(i));
      }
    }
    return ret;

  }
}
