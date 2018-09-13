package it.okkam.opendata.geonames.flink;

import static it.okkam.opendata.geonames.GeoNamesUtils.ALTNAMES_COL_LANG;

import it.okkam.opendata.geonames.GeoNamesUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class AltNamesSelector implements FlatMapFunction<Row, Row> {
  private static final long serialVersionUID = 1L;
  private final Set<String> langsToFilter;
  private final int langPos;

  public AltNamesSelector(String[] langsToFilter, Map<String, Integer> altNamesFieldPos) {
    this.langPos = altNamesFieldPos.get(ALTNAMES_COL_LANG);
    this.langsToFilter = new HashSet<>(Arrays.asList(langsToFilter));
  }

  @Override
  public void flatMap(Row row, Collector<Row> out) throws Exception {
    final String langType = row.getField(langPos) == null ? //
        GeoNamesUtils.LANG_DEFAULT : (String) row.getField(langPos);
    if (langsToFilter.contains(langType)) {
      out.collect(row);
    }

  }
}
