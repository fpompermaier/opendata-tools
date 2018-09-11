package it.okkam.opendata.geonames.flink;

import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_ADM1;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_ADM2;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_ADM3;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_ADM4;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_COUNTRY_CODE;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_FEATURE_CLASS;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_FEATURE_CODE;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_POPULATION;
import static it.okkam.opendata.geonames.GeoNamesUtils.COL_GEONAMES_ID;
import static it.okkam.opendata.geonames.GeoNamesUtils.COL_GEONAMES_URL;
import static it.okkam.opendata.geonames.GeoNamesUtils.SUFFIX_CODE_COL;
import static it.okkam.opendata.geonames.GeoNamesUtils.getAllCountriesInFieldNames;
import static it.okkam.opendata.geonames.GeoNamesUtils.getFieldPosMap;
import static it.okkam.opendata.geonames.GeoNamesUtils.getGeonamesUrl;

import it.okkam.opendata.geonames.GeoNamesUtils;
import it.okkam.opendata.geonames.flink.model.LabelledRow;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class AllCountriesGenerator implements FlatMapFunction<Row, LabelledRow> {

  private static final long serialVersionUID = 1L;

  private final String[] allCountriesOutFn;
  private final int geonamesIdPos;
  private final int geonamesUrlPos;
  private final int featureClassPos;
  private final int featureCodePos;
  private final int countryCodePos;
  private final int adm1Pos;
  private final int adm1CodePos;
  private final int adm2Pos;
  private final int adm2CodePos;
  private final int adm3Pos;
  private final int adm3CodePos;
  private final int adm4Pos;
  private final int adm4CodePos;
  private final int populationPos;

  private final Map<String, Integer> allCountriesOutFpos;
  private final Map<String, String[]> filtersMap;

  private final Row reuse;

  public AllCountriesGenerator(String[] allCountriesExtraFn, Map<String, String[]> filtersMap) {
    final String[] allCountriesInFn = getAllCountriesInFieldNames();
    this.reuse = new Row(allCountriesInFn.length + allCountriesExtraFn.length);// add extra fields
    this.allCountriesOutFn = GeoNamesUtils.joinArrays(allCountriesInFn, allCountriesExtraFn);
    this.allCountriesOutFpos = getFieldPosMap(allCountriesOutFn);
    this.filtersMap = filtersMap;
    this.geonamesIdPos = allCountriesOutFpos.get(COL_GEONAMES_ID);
    this.geonamesUrlPos = allCountriesOutFpos.get(COL_GEONAMES_URL);
    this.featureClassPos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_FEATURE_CLASS);
    this.featureCodePos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_FEATURE_CODE);
    this.populationPos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_POPULATION);
    this.countryCodePos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_COUNTRY_CODE);
    this.adm1Pos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_ADM1);
    this.adm2Pos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_ADM2);
    this.adm3Pos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_ADM3);
    this.adm4Pos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_ADM4);

    this.adm1CodePos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_ADM1 + SUFFIX_CODE_COL);
    this.adm2CodePos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_ADM2 + SUFFIX_CODE_COL);
    this.adm3CodePos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_ADM3 + SUFFIX_CODE_COL);
    this.adm4CodePos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_ADM4 + SUFFIX_CODE_COL);
  }


  @Override
  public void flatMap(Row row, Collector<LabelledRow> out) throws Exception {
    for (Entry<String, String[]> filterEntry : filtersMap.entrySet()) {
      if (checkFilterConf(row, filterEntry.getValue())) {
        for (int i = 0; i < row.getArity(); i++) {
          Object rowValue = row.getField(i);
          final boolean isNullAdm1 = i == adm1Pos && "00".equals(rowValue);
          final boolean isNullPopulation = i == populationPos && "0".equals(rowValue);
          if (isNullAdm1 || isNullPopulation) {
            rowValue = null;
          }
          reuse.setField(i, rowValue);
        }
        fillExtraFields();
        out.collect(new LabelledRow(filterEntry.getKey(), reuse));
      }
    }

  }


  private void fillExtraFields() {
    reuse.setField(adm1CodePos, null);
    reuse.setField(adm2CodePos, null);
    reuse.setField(adm3CodePos, null);
    reuse.setField(adm4CodePos, null);
    reuse.setField(geonamesUrlPos, getGeonamesUrl((String) reuse.getField(geonamesIdPos)));
    final String country = (String) reuse.getField(countryCodePos);
    final String adm1 = (String) reuse.getField(adm1Pos);
    final String adm2 = (String) reuse.getField(adm2Pos);
    final String adm3 = (String) reuse.getField(adm3Pos);
    final String adm4 = (String) reuse.getField(adm4Pos);
    final boolean adm1Empty = adm1 == null || adm1.isEmpty();
    final boolean adm2Empty = adm2 == null || adm2.isEmpty();
    final boolean adm3Empty = adm3 == null || adm3.isEmpty();
    final boolean adm4Empty = adm4 == null || adm4.isEmpty();

    if (!adm1Empty) {
      final String adm1Code = country + "." + adm1;
      reuse.setField(adm1CodePos, adm1Code);
      if (!adm2Empty) {
        final String adm2Code = adm1Code + "." + adm2;
        reuse.setField(adm2CodePos, adm2Code);
        if (!adm3Empty) {
          final String adm3Code = adm2Code + "." + adm3;
          reuse.setField(adm3CodePos, adm3Code);
          if (!adm4Empty) {
            final String adm4Code = adm3Code + "." + adm4;
            reuse.setField(adm4CodePos, adm4Code);
          }
        }
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

