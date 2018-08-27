package it.okkam.opendata.geonames;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class GeoNamesConstants {

  private GeoNamesConstants() {
    throw new IllegalArgumentException("Utility class");
  }

  public static final String SEPARATOR_PATTERN = Pattern.quote("|");
  public static final Character FIELD_DELIM = '\t';
  public static final String FIELD_DELIM_STR = FIELD_DELIM + "";

  public static final String LANG_DEFAULT = "default";
  public static final String LANG_IT = "it";
  public static final String LANG_DE = "de";
  public static final String LANG_EN = "en";
  public static final String LANG_FR = "fr";
  public static final String LANG_RU = "ru";
  public static final String LANG_ES = "es";
  public static final String LANG_PT = "pt";
  public static final String LANG_LINK = "link";
  public static final String LANG_WIKIDATA = "wkdt";
  public static final String LANG_FR1973 = "fr_1793";
  public static final String LANG_POSTCODE = "post";

  // keep geonames url always as first field when used
  public static final int GEONAMES_URL_FIELD_POS = 0;

  public static final String ALL_COUNTRIES_COL_GEONAMES_URL = "GEONAMES_URL";
  public static final String ALL_COUNTRIES_COL_GEONAMES_ID = "GEONAMES_ID";
  public static final String ALL_COUNTRIES_COL_NAME = "NAME";
  public static final String ALL_COUNTRIES_COL_NAME_ASCII = "ASCIINAME";
  public static final String ALL_COUNTRIES_COL_ALT_NAMES = "ALT_NAMES";
  public static final String ALL_COUNTRIES_COL_LAT = "LAT";
  public static final String ALL_COUNTRIES_COL_LON = "LON";
  public static final String ALL_COUNTRIES_COL_FEATURE_CLASS = "FEAT_CLASS";
  public static final String ALL_COUNTRIES_COL_FEATURE_CODE = "FEAT_CODE";
  public static final String ALL_COUNTRIES_COL_COUNTRY_CODE = "COUNTRY_CODE";
  public static final String ALL_COUNTRIES_COL_CC2 = "CC2";
  public static final String ALL_COUNTRIES_COL_ADM1 = "ADM1";
  public static final String ALL_COUNTRIES_COL_ADM2 = "ADM2";
  public static final String ALL_COUNTRIES_COL_ADM3 = "ADM3";
  public static final String ALL_COUNTRIES_COL_ADM4 = "ADM4";
  public static final String ALL_COUNTRIES_COL_POPULATION = "POPULATION";
  public static final String ALL_COUNTRIES_COL_ELEVATION = "ELEVATION";
  public static final String ALL_COUNTRIES_COL_DEM = "DEM";
  public static final String ALL_COUNTRIES_COL_TIMEZONE = "TIMEZONE";
  public static final String ALL_COUNTRIES_COL_LAST_UPDATE = "LASTUPDATE";


  public static String[] getAllCountriesInFieldNames() {
    return new String[] {//
        ALL_COUNTRIES_COL_GEONAMES_ID, //
        ALL_COUNTRIES_COL_NAME, //
        ALL_COUNTRIES_COL_NAME_ASCII, //
        ALL_COUNTRIES_COL_ALT_NAMES, //
        ALL_COUNTRIES_COL_LAT, //
        ALL_COUNTRIES_COL_LON, //
        ALL_COUNTRIES_COL_FEATURE_CLASS, //
        ALL_COUNTRIES_COL_FEATURE_CODE, //
        ALL_COUNTRIES_COL_COUNTRY_CODE, //
        ALL_COUNTRIES_COL_CC2, //
        ALL_COUNTRIES_COL_ADM1, //
        ALL_COUNTRIES_COL_ADM2, //
        ALL_COUNTRIES_COL_ADM3, //
        ALL_COUNTRIES_COL_ADM4, //
        ALL_COUNTRIES_COL_POPULATION, //
        ALL_COUNTRIES_COL_ELEVATION, //
        ALL_COUNTRIES_COL_DEM, //
        ALL_COUNTRIES_COL_TIMEZONE, //
        ALL_COUNTRIES_COL_LAST_UPDATE};
  }


  public static final String ALTNAMES_COL_GEONAMES_URL = "GEONAMES_URL";
  public static final String ALTNAMES_COL_ID = "ALTNAME_ID";
  public static final String ALTNAMES_COL_GEONAMES_ID = "GEONAMES_ID";
  public static final String ALTNAMES_COL_LANG = "LANG";
  public static final String ALTNAMES_COL_NAME = "VALUE";
  public static final String ALTNAMES_COL_PREFERRED = "PREFERRED";
  public static final String ALTNAMES_COL_SHORT = "SHORT";
  public static final String ALTNAMES_COL_COLLOQUIAL = "COLLOQUIAL";
  public static final String ALTNAMES_COL_HISTORICAL = "HISTORICAL";
  public static final String ALTNAMES_COL_FROM = "FROM";
  public static final String ALTNAMES_COL_TO = "TO";

  public static String[] getAltNamesInFieldNames() {
    return new String[] {//
        ALTNAMES_COL_ID, //
        ALTNAMES_COL_GEONAMES_ID, //
        ALTNAMES_COL_LANG, //
        ALTNAMES_COL_NAME, //
        ALTNAMES_COL_PREFERRED, //
        ALTNAMES_COL_SHORT, //
        ALTNAMES_COL_COLLOQUIAL, //
        ALTNAMES_COL_HISTORICAL, //
        ALTNAMES_COL_FROM, //
        ALTNAMES_COL_TO//
    };
  }

  public static String getGeonamesUrl(String geonamesId) {
    return "http://www.geonames.org/" + geonamesId;
  }

  public static String getWikidataUrl(String wikiDataEntityId) {
    return "http://www.wikidata.org/entity/" + wikiDataEntityId;
  }

  public static Map<String, Integer> getFieldPosMap(String[] fieldNames) {
    final Map<String, Integer> ret = new HashMap<>();
    int colPos = 0;
    for (String fn : fieldNames) {
      ret.put(fn, colPos++);
    }
    return ret;
  }
}
