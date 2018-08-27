package it.okkam.opendata.geonames;

import static it.okkam.opendata.geonames.GeoNamesConstants.ALTNAMES_COL_GEONAMES_ID;
import static it.okkam.opendata.geonames.GeoNamesConstants.ALTNAMES_COL_LANG;
import static it.okkam.opendata.geonames.GeoNamesConstants.ALTNAMES_COL_NAME;
import static it.okkam.opendata.geonames.GeoNamesConstants.GEONAMES_URL_FIELD_POS;
import static it.okkam.opendata.geonames.GeoNamesConstants.getGeonamesUrl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class AltNamesGenerator implements GroupReduceFunction<Row, Row> {
  private static final long serialVersionUID = 1L;
  private final Set<String> langsToFilter;
  private final Map<String, Set<String>> langValuesMap = new HashMap<>();
  private final Row reuse;
  private final int geonamesIdPos;
  private final int langPos;
  private final int langValPos;

  public AltNamesGenerator(String[] langsToFilter, String[] altNamesOutFn,
      Map<String, Integer> altNamesInFpos) {
    this.geonamesIdPos = altNamesInFpos.get(ALTNAMES_COL_GEONAMES_ID);
    this.langPos = altNamesInFpos.get(ALTNAMES_COL_LANG);
    this.langValPos = altNamesInFpos.get(ALTNAMES_COL_NAME);
    this.langsToFilter = new HashSet<>(Arrays.asList(langsToFilter));
    this.reuse = new Row(altNamesOutFn.length);
  }

  @Override
  public void reduce(Iterable<Row> rows, Collector<Row> out) throws Exception {
    langValuesMap.clear();
    reuse.setField(GEONAMES_URL_FIELD_POS, null);
    for (Row row : rows) {
      final String value = (String) row.getField(langValPos);
      String langType = (String) row.getField(langPos);
      if (langType == null) {
        langType = GeoNamesConstants.LANG_DEFAULT;
      }
      if (langsToFilter.contains(langType)) {
        Set<String> langValues = langValuesMap.getOrDefault(langType, new HashSet<>());
        langValues.add(value);
        langValuesMap.put(langType, langValues);
      }
      // compute geonames url only once per group of rows
      if (reuse.getField(GEONAMES_URL_FIELD_POS) == null) {
        reuse.setField(GEONAMES_URL_FIELD_POS,
            getGeonamesUrl((String) row.getField(geonamesIdPos)));
      }
    }
    int i = 1;
    for (String targetColumn : langsToFilter) {
      Set<String> langValues = langValuesMap.getOrDefault(targetColumn, null);
      reuse.setField(i++, langValues == null ? null : String.join(",", langValues));
    }
    out.collect(reuse);
  }
}
