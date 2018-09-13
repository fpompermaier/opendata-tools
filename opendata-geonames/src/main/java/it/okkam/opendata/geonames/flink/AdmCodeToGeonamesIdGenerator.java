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
import static it.okkam.opendata.geonames.GeoNamesUtils.COL_GEONAMES_ID;

import it.okkam.opendata.geonames.GeoNamesUtils;
import it.okkam.opendata.geonames.flink.model.AdmCodeTuple;

import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class AdmCodeToGeonamesIdGenerator implements FlatMapFunction<Row, AdmCodeTuple> {

  private static final long serialVersionUID = 1L;
  private final String targetFeatClass;
  private final String targetFeatCode;
  private final int countryCodePos;
  private final int adm1Pos;
  private final int adm2Pos;
  private final int adm3Pos;
  private final int adm4Pos;
  private final int adm5Pos;
  private final int featClassPos;
  private final int featCodePos;
  private final int geonamesIdPos;
  private final int lastUpdatePos;

  public AdmCodeToGeonamesIdGenerator(String[] allCountriesInFn, String targetFeatCode) {
    this.targetFeatClass = GeoNamesUtils.FEAT_CLASS_ADM;
    this.targetFeatCode = targetFeatCode;
    final Map<String, Integer> allCountriesInFpos = GeoNamesUtils.getFieldPosMap(allCountriesInFn);
    this.featClassPos = allCountriesInFpos.get(ALL_COUNTRIES_COL_FEATURE_CLASS);
    this.featCodePos = allCountriesInFpos.get(ALL_COUNTRIES_COL_FEATURE_CODE);
    this.geonamesIdPos = allCountriesInFpos.get(COL_GEONAMES_ID);
    this.countryCodePos = allCountriesInFpos.get(ALL_COUNTRIES_COL_COUNTRY_CODE);
    this.adm1Pos = allCountriesInFpos.get(ALL_COUNTRIES_COL_ADM1);
    this.adm2Pos = allCountriesInFpos.get(ALL_COUNTRIES_COL_ADM2);
    this.adm3Pos = allCountriesInFpos.get(ALL_COUNTRIES_COL_ADM3);
    this.adm4Pos = allCountriesInFpos.get(ALL_COUNTRIES_COL_ADM4);
    this.adm5Pos = allCountriesInFpos.get(ADM5_COL_ADM5);
    this.lastUpdatePos = allCountriesInFpos.get(ALL_COUNTRIES_COL_LAST_UPDATE);
  }

  @Override
  public void flatMap(Row row, Collector<AdmCodeTuple> out) throws Exception {
    final String featClass = (String) row.getField(featClassPos);
    final String featCode = (String) row.getField(featCodePos);
    if (featClass == null || !targetFeatClass.equals(featClass)) {
      return;
    }
    if (featCode == null || !targetFeatCode.equals(featCode)) {
      return;
    }
    final String geonamesId = (String) row.getField(geonamesIdPos);
    final String country = (String) row.getField(countryCodePos);
    final String adm1 = (String) row.getField(adm1Pos);
    final String adm2 = (String) row.getField(adm2Pos);
    final String adm3 = (String) row.getField(adm3Pos);
    final String adm4 = (String) row.getField(adm4Pos);
    final String adm5 = (String) row.getField(adm5Pos);
    final String lastUpdate = (String) row.getField(lastUpdatePos);
    final String admCode = getAdmCode(country, adm1, adm2, adm3, adm4, adm5);
    out.collect(new AdmCodeTuple(admCode, geonamesId, lastUpdate));
  }

  private String getAdmCode(String country, String adm1, String adm2, String adm3, String adm4, String adm5) {
    final boolean adm1Empty = adm1 == null || adm1.isEmpty();
    final boolean adm2Empty = adm2 == null || adm2.isEmpty();
    final boolean adm3Empty = adm3 == null || adm3.isEmpty();
    final boolean adm4Empty = adm4 == null || adm4.isEmpty();
    final boolean adm5Empty = adm5 == null || adm5.isEmpty();

    if (!adm1Empty) {
      final String adm1Code = country + "." + adm1;
      if (targetFeatCode.equals(GeoNamesUtils.FEAT_CODE_ADM1)) {
        return adm1Code;
      }
      if (!adm2Empty) {
        final String adm2Code = adm1Code + "." + adm2;
        if (targetFeatCode.equals(GeoNamesUtils.FEAT_CODE_ADM2)) {
          return adm2Code;
        }
        if (!adm3Empty) {
          final String adm3Code = adm2Code + "." + adm3;
          if (targetFeatCode.equals(GeoNamesUtils.FEAT_CODE_ADM3)) {
            return adm3Code;
          }
          if (!adm4Empty) {
            final String adm4Code = adm3Code + "." + adm4;
            if (targetFeatCode.equals(GeoNamesUtils.FEAT_CODE_ADM4)) {
              return adm4Code;
            }
            if (!adm5Empty) {
              final String adm5Code = adm4Code + "." + adm5;
              if (targetFeatCode.equals(GeoNamesUtils.FEAT_CODE_ADM5)) {
                return adm5Code;
              }
            }
          }
        }
      }
    }
    return GeoNamesUtils.FEAT_CODE_ERROR;
  }
}
