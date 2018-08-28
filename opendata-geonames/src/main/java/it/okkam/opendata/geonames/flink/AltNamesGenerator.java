package it.okkam.opendata.geonames.flink;

import static it.okkam.opendata.geonames.GeoNamesUtils.ALTNAMES_COL_LANG;

import it.okkam.opendata.geonames.GeoNamesUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class AltNamesGenerator implements FlatMapFunction<Row, Row> {
  private static final long serialVersionUID = 1L;
  private final Set<String> langsToFilter;
  private final Row reuse;
  private final int langPos;
  private final Map<String, Integer> altNamesOutFieldPos;
  private final Map<Integer, String> altNamesInFieldPos;

  public AltNamesGenerator(String[] langsToFilter, String[] altNamesOutFn,
      Map<String, Integer> altNamesInFpos) {
    this.reuse = new Row(altNamesOutFn.length);
    this.langPos = altNamesInFpos.get(ALTNAMES_COL_LANG);
    this.langsToFilter = new HashSet<>(Arrays.asList(langsToFilter));
    this.altNamesOutFieldPos = GeoNamesUtils.getFieldPosMap(altNamesOutFn);
    this.altNamesInFieldPos = new HashMap<>();
    for (Entry<String, Integer> fieldPos : altNamesInFpos.entrySet()) {
      altNamesInFieldPos.put(fieldPos.getValue(), fieldPos.getKey());
    }
  }

  private void clearReuseRow() {
    for (int i = 0; i < reuse.getArity(); i++) {
      reuse.setField(i, null);
    }
  }

  @Override
  public void flatMap(Row row, Collector<Row> out) throws Exception {
    clearReuseRow();
    final String langType = row.getField(langPos) == null ? //
        GeoNamesUtils.LANG_DEFAULT : (String) row.getField(langPos);
    if (langsToFilter.contains(langType)) {
      for (int i = 0; i < row.getArity(); i++) {
        final String fieldName = altNamesInFieldPos.get(i);
        final Integer outFieldPos = altNamesOutFieldPos.get(fieldName);
        if (outFieldPos != null) {
          reuse.setField(outFieldPos, row.getField(i));
        }
      }
      out.collect(reuse);
    }

  }
}
