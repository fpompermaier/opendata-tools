package it.okkam.opendata.geonames.flink;

import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_COUNTRY_CODE;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_FEATURE_CLASS;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_FEATURE_CODE;
import static it.okkam.opendata.geonames.GeoNamesUtils.COL_GEONAMES_ID;
import static it.okkam.opendata.geonames.GeoNamesUtils.getGeonamesUrl;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class AllCountriesGenerator implements FlatMapFunction<Row, Tuple2<String, Row>> {

  private static final long serialVersionUID = 1L;

  private final String[] allCountriesOutFn;
  private final int geonameIdPos;
  private final int featureClassPos;
  private final int featureCodePos;
  private final int countryCodePos;
  private final Row reuse;

  private final Map<String, Integer> allCountriesInFpos;
  private final Map<String, String[]> filtersMap;

  public AllCountriesGenerator(Map<String, Integer> allCountriesInFpos, String[] allCountriesOutFn,
      Map<String, String[]> filtersMap) {
    this.reuse = new Row(allCountriesOutFn.length);
    this.allCountriesInFpos = allCountriesInFpos;
    this.allCountriesOutFn = allCountriesOutFn;
    this.filtersMap = filtersMap;
    this.geonameIdPos = allCountriesInFpos.get(COL_GEONAMES_ID);
    this.featureClassPos = allCountriesInFpos.get(ALL_COUNTRIES_COL_FEATURE_CLASS);
    this.featureCodePos = allCountriesInFpos.get(ALL_COUNTRIES_COL_FEATURE_CODE);
    this.countryCodePos = allCountriesInFpos.get(ALL_COUNTRIES_COL_COUNTRY_CODE);
  }

  @Override
  public void flatMap(Row row, Collector<Tuple2<String, Row>> out) throws Exception {
    for (Entry<String, String[]> filterEntry : filtersMap.entrySet()) {
      if (checkFilterConf(row, filterEntry.getValue())) {
        reuse.setField(0, getGeonamesUrl((String) row.getField(geonameIdPos)));
        for (int i = 1; i < reuse.getArity(); i++) {
          final Integer fieldPos = allCountriesInFpos.get(allCountriesOutFn[i]);
          reuse.setField(i, fieldPos == null ? null : row.getField(fieldPos));
        }
        out.collect(new Tuple2<>(filterEntry.getKey(), reuse));
      }
    }
  }

  // return true iff feature class and code and country match the passed filter
  private boolean checkFilterConf(Row row, String[] filter) {
    final String featureClass = (String) row.getField(featureClassPos);
    final String featureCode = (String) row.getField(featureCodePos);
    final String countryCode = (String) row.getField(countryCodePos);
    if (filter.length < 3) {
      final boolean validFeatureClass = filter[0].equals("*") || filter[0].equals(featureClass);
      final boolean validFeatureCode = filter[1].equals("*") || filter[1].equals(featureCode);
      return validFeatureClass && validFeatureCode;
    }
    final boolean validCountryCode = filter[0].equals("*") || filter[0].equals(countryCode);
    final boolean validFeatureClass = filter[1].equals("*") || filter[1].equals(featureClass);
    final boolean validFeatureCode = filter[2].equals("*") || filter[2].equals(featureCode);
    return validFeatureClass && validFeatureCode && validCountryCode;
  }
}

