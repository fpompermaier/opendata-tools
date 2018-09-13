package it.okkam.opendata.geonames.flink;

import static it.okkam.opendata.geonames.GeoNamesUtils.ADM5_COL_ADM5;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_ADM1;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_ADM2;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_ADM3;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_ADM4;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_COUNTRY_CODE;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_FEATURE_CLASS;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_FEATURE_CODE;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_LAST_UPDATE;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_POPULATION;
import static it.okkam.opendata.geonames.GeoNamesUtils.COL_GEONAMES_ID;
import static it.okkam.opendata.geonames.GeoNamesUtils.COL_GEONAMES_URL;
import static it.okkam.opendata.geonames.GeoNamesUtils.COL_STATE_ID;
import static it.okkam.opendata.geonames.GeoNamesUtils.FEAT_CODE_ADM1;
import static it.okkam.opendata.geonames.GeoNamesUtils.FEAT_CODE_ADM2;
import static it.okkam.opendata.geonames.GeoNamesUtils.FEAT_CODE_ADM3;
import static it.okkam.opendata.geonames.GeoNamesUtils.FEAT_CODE_ADM4;
import static it.okkam.opendata.geonames.GeoNamesUtils.FEAT_CODE_ADM5;
import static it.okkam.opendata.geonames.GeoNamesUtils.PARENT_LOCATIONS_FILTER;
import static it.okkam.opendata.geonames.GeoNamesUtils.SUFFIX_CODE_COL;
import static it.okkam.opendata.geonames.GeoNamesUtils.getGeonamesUrl;
import static it.okkam.opendata.geonames.GeoNamesUtils.getStateId;

import it.okkam.opendata.geonames.GeoNamesUtils;
import it.okkam.opendata.geonames.flink.model.LabelledRow;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class AllCountriesEnricher implements FlatMapFunction<Row, LabelledRow> {

  private static final long serialVersionUID = 1L;

  private final int geonamesIdPos;
  private final int geonamesUrlPos;
  private final int stateIdPos;
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
  private final int adm5Pos;
  private final int adm5CodePos;
  private final int populationPos;
  private final int lastUpdatePos;

  private final Map<String, String[]> filtersMap;
  private final Set<String> countryParentIds;
  private final String targetCountry;

  private final Row reuse;

  public AllCountriesEnricher(Map<String, Integer> allCountriesOutFpos,
      Map<String, String[]> filtersMap, String targetCountry, Set<String> countryParentIds) {
    this.reuse = new Row(allCountriesOutFpos.size());
    this.countryParentIds = countryParentIds;
    this.targetCountry = targetCountry;
    this.filtersMap = filtersMap;
    this.geonamesIdPos = allCountriesOutFpos.get(COL_GEONAMES_ID);
    this.geonamesUrlPos = allCountriesOutFpos.get(COL_GEONAMES_URL);
    this.stateIdPos = allCountriesOutFpos.get(COL_STATE_ID);
    this.featureClassPos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_FEATURE_CLASS);
    this.featureCodePos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_FEATURE_CODE);
    this.populationPos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_POPULATION);
    this.countryCodePos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_COUNTRY_CODE);
    this.adm1Pos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_ADM1);
    this.adm2Pos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_ADM2);
    this.adm3Pos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_ADM3);
    this.adm4Pos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_ADM4);
    this.adm5Pos = allCountriesOutFpos.get(ADM5_COL_ADM5);
    this.lastUpdatePos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_LAST_UPDATE);

    this.adm1CodePos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_ADM1 + SUFFIX_CODE_COL);
    this.adm2CodePos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_ADM2 + SUFFIX_CODE_COL);
    this.adm3CodePos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_ADM3 + SUFFIX_CODE_COL);
    this.adm4CodePos = allCountriesOutFpos.get(ALL_COUNTRIES_COL_ADM4 + SUFFIX_CODE_COL);
    this.adm5CodePos = allCountriesOutFpos.get(ADM5_COL_ADM5 + SUFFIX_CODE_COL);
  }


  @Override
  public void flatMap(Row row, Collector<LabelledRow> out) throws Exception {
    for (Entry<String, String[]> filterEntry : filtersMap.entrySet()) {
      String filterLabel = null;
      final String geonamesId = (String) row.getField(geonamesIdPos);
      // parents of target country should be immune to filter
      if (countryParentIds.contains(geonamesId)) {
        filterLabel = GeoNamesUtils.PARENT_LOCATIONS_FILTER;
      } else if (checkFilterConf(row, targetCountry, filterEntry)) {
        filterLabel = filterEntry.getKey();
      }
      if (filterLabel != null) {
        copyCleanedRowValues(row);
        fillExtraFields();
        out.collect(new LabelledRow(filterLabel, reuse));
        break;//first matching filter wins
      }
    }
  }


  private void copyCleanedRowValues(Row row) {
    for (int i = 0; i < row.getArity(); i++) {
      reuse.setField(i, row.getField(i));
    }
    final String adm1 = (String) reuse.getField(adm1Pos);
    final String population = (String) reuse.getField(populationPos);
    reuse.setField(adm1Pos, "00".equals(adm1) ? null : adm1);
    reuse.setField(populationPos, "0".equals(population) ? null : population);

  }

  private void fillExtraFields() {
    final String featCode = (String) reuse.getField(featureCodePos);
    final String lastupdate = (String) reuse.getField(lastUpdatePos);
    final String geonamesId = (String) reuse.getField(geonamesIdPos);
    final String country = (String) reuse.getField(countryCodePos);
    reuse.setField(geonamesUrlPos, getGeonamesUrl(geonamesId));
    reuse.setField(stateIdPos, getStateId(geonamesId, lastupdate));
    fillAdms(featCode, country);
  }


  private void fillAdms(final String featCode, final String country) {
    final String adm1 = (String) reuse.getField(adm1Pos);
    final String adm2 = (String) reuse.getField(adm2Pos);
    final String adm3 = (String) reuse.getField(adm3Pos);
    final String adm4 = (String) reuse.getField(adm4Pos);
    final String adm5 = (String) reuse.getField(adm5Pos);

    // clear reused values
    reuse.setField(adm1CodePos, null);
    reuse.setField(adm2CodePos, null);
    reuse.setField(adm3CodePos, null);
    reuse.setField(adm4CodePos, null);
    reuse.setField(adm5CodePos, null);

    // FEAT_CODE_ADM check avoid to set an administration as parent admin of itself
    if (adm1 != null && !adm1.isEmpty() && !FEAT_CODE_ADM1.equals(featCode)) {
      final String adm1Code = country + "." + adm1;
      reuse.setField(adm1CodePos, adm1Code);
      if (adm2 != null && !adm2.isEmpty() && !FEAT_CODE_ADM2.equals(featCode)) {
        final String adm2Code = adm1Code + "." + adm2;
        reuse.setField(adm2CodePos, adm2Code);
        if (adm3 != null && !adm3.isEmpty() && !FEAT_CODE_ADM3.equals(featCode)) {
          final String adm3Code = adm2Code + "." + adm3;
          reuse.setField(adm3CodePos, adm3Code);
          if (adm4 != null && !adm4.isEmpty() && !FEAT_CODE_ADM4.equals(featCode)) {
            final String adm4Code = adm3Code + "." + adm4;
            reuse.setField(adm4CodePos, adm4Code);
            if (adm5 != null && !adm5.isEmpty() && !FEAT_CODE_ADM5.equals(featCode)) {
              final String adm5Code = adm4Code + "." + adm5;
              reuse.setField(adm5CodePos, adm5Code);
            }
          }
        }
      }
    }
  }



  // return true iff feature class and code and country match the passed filter
  private boolean checkFilterConf(Row row, String targetCountry, Entry<String, String[]> filter) {
    if (filter.getKey().equals(PARENT_LOCATIONS_FILTER)) {
      return false;
    }
    final String featureClass = (String) row.getField(featureClassPos);
    final String featureCode = (String) row.getField(featureCodePos);
    final String countryCode = (String) row.getField(countryCodePos);
    if (!targetCountry.equals(countryCode)) {
      return false;
    }
    final String validFclass = filter.getKey().substring(0, filter.getKey().indexOf('=')).trim();
    if (!validFclass.equals(featureClass)) {
      return false;
    }
    for (int i = 0; i < filter.getValue().length; i++) {
      final String validFcode = filter.getValue()[i];
      if (validFcode.trim().equals("*") || validFcode.trim().equals(featureCode)) {
        return true;
      }
    }
    return false;
  }

}

