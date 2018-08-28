package it.okkam.opendata.geonames;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class GeoNamesUtils {

  private GeoNamesUtils() {
    throw new IllegalArgumentException("Utility class");
  }

  private static final String IPV4_PATTERN =
      "(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])";
  private static final Pattern VALID_IPV4_PATTERN =
      Pattern.compile(IPV4_PATTERN, Pattern.CASE_INSENSITIVE);

  public static final String OUT_FILENAME_LOCATIONS = "locations.tsv";
  public static final String OUT_FILENAME_ALTNAMES = "altNames.tsv";
  public static final String OUT_FILENAME_LINKS = "links.tsv";

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

  public static final String COL_GEONAMES_URL = "GEONAMES_URL";
  public static final String COL_GEONAMES_ID = "GEONAMES_ID";
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
        COL_GEONAMES_ID, //
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


  public static final String ALTNAMES_COL_ID = "ALTNAME_ID";
  public static final String ALTNAMES_COL_LANG = "LANG";
  public static final String ALTNAMES_COL_NAME = "VALUE";
  public static final String ALTNAMES_COL_PREFERRED = "PREFERRED";
  public static final String ALTNAMES_COL_SHORT = "SHORT";
  public static final String ALTNAMES_COL_COLLOQUIAL = "COLLOQUIAL";
  public static final String ALTNAMES_COL_HISTORICAL = "HISTORICAL";
  public static final String ALTNAMES_COL_FROM = "FROM";
  public static final String ALTNAMES_COL_TO = "TO";

  public static final String LINKS_COL_DOMAIN = "LINK_DOMAIN";
  public static final String LINKS_COL_LANG = "LINK_LANG";

  public static String[] getAltNamesInFieldNames() {
    return new String[] {//
        ALTNAMES_COL_ID, //
        COL_GEONAMES_ID, //
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

  public static List<List<String>> getAltNamesEnrichedFieldNames() {
    List<List<String>> ret = new ArrayList<>();
    List<String> locationColumns = new ArrayList<>();
    locationColumns.add(COL_GEONAMES_URL);
    locationColumns.add(ALL_COUNTRIES_COL_NAME);
    locationColumns.add(ALL_COUNTRIES_COL_FEATURE_CLASS);
    locationColumns.add(ALL_COUNTRIES_COL_FEATURE_CODE);
    ret.add(locationColumns);

    List<String> altNameColumns = new ArrayList<>();
    altNameColumns.add(ALTNAMES_COL_LANG);
    altNameColumns.add(ALTNAMES_COL_NAME);
    altNameColumns.add(ALTNAMES_COL_PREFERRED);
    altNameColumns.add(ALTNAMES_COL_SHORT);
    altNameColumns.add(ALTNAMES_COL_COLLOQUIAL);
    altNameColumns.add(ALTNAMES_COL_HISTORICAL);
    ret.add(altNameColumns);
    return ret;
  }

  public static Map<String, String> getOutFileNamesMap(String outDir,
      Map<String, String[]> filtersMap) {
    final Map<String, String> ret = new HashMap<>();
    for (String filterStr : filtersMap.keySet()) {
      final String[] tokens = filterStr.split(SEPARATOR_PATTERN);
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].equals("*") ? "ALL" : tokens[i];
      }
      ret.put(filterStr, "file:" + java.nio.file.Paths.get(outDir, tokens) + "/");
    }
    return ret;
  }

  public static Map<String, String[]> getFiltersMap(String[] featuresFilter) {
    final Map<String, String[]> ret = new HashMap<>();
    for (int i = 0; i < featuresFilter.length; i++) {
      ret.put(featuresFilter[i], featuresFilter[i].split(SEPARATOR_PATTERN));
    }
    return ret;
  }


  public static String getGeonamesUrl(String geonamesId) {
    return "http://www.geonames.org/" + geonamesId;
  }

  public static String getWikidataUrl(String wikiDataEntityId) {
    return "http://www.wikidata.org/entity/" + wikiDataEntityId;
  }

  public static String[] joinArrays(String[] left, String[] right) {
    if (left == null || right == null) {
      throw new IllegalArgumentException("Both arrays must be non-null");
    }
    final String[] ret = new String[left.length + right.length];
    System.arraycopy(left, 0, ret, 0, left.length);
    System.arraycopy(right, 0, ret, left.length, right.length);
    return ret;
  }

  public static Map<String, Integer> getFieldPosMap(String[] fieldNames) {
    final Map<String, Integer> ret = new HashMap<>();
    int colPos = 0;
    for (String fn : fieldNames) {
      ret.put(fn, colPos++);
    }
    return ret;
  }

  /**
   * Determine if the given string is a valid IPv4 address. This method uses pattern matching to see
   * if the given string could be a valid IP address.
   *
   * @param ipAddress A string that is to be examined to verify whether or not it could be a valid
   *        IP address.
   * @return <code>true</code> if the string is a value that is a valid IP address,
   *         <code>false</code> otherwise.
   */
  public static boolean isIpAddress(String ipAddress) {
    return VALID_IPV4_PATTERN.matcher(ipAddress).matches();
  }

  public static String getLinkDomain(final String[] hostSplit) {
    final StringBuilder linkDomain = new StringBuilder(hostSplit[hostSplit.length - 1]);
    if (hostSplit.length >= 2) {
      linkDomain.insert(0, hostSplit[hostSplit.length - 2] + ".");
    }
    return linkDomain.toString();
  }

  public static String getLinkLang(final String[] hostSplit) {
    String lang = null;
    if ("wikipedia".equals(hostSplit[1])) {
      lang = hostSplit[0];
    }
    return lang;
  }
}
