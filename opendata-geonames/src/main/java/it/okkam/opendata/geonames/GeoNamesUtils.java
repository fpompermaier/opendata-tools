package it.okkam.opendata.geonames;

import it.okkam.flink.csv.CsvLine2Row;
import it.okkam.opendata.geonames.flink.AdmCodeToGeonamesIdGenerator;
import it.okkam.opendata.geonames.flink.AltNamesSelector;
import it.okkam.opendata.geonames.flink.FlinkUtils;
import it.okkam.opendata.geonames.flink.RowEnlarger;
import it.okkam.opendata.geonames.flink.RowFieldsProjector;
import it.okkam.opendata.geonames.flink.model.AdmCodeTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

public class GeoNamesUtils {


  private GeoNamesUtils() {
    throw new IllegalArgumentException("Utility class");
  }

  public static final String BASE_DIR = "file:/media/okkam/FLASSD/datalinks/datasets/geonames/";
  public static final String OUT_DIR = "/tmp/geonames/";

  private static final String ALT_NAMES_FILENAME = "alternateNamesV2.txt";
  private static final String ALL_COUNTRIES_FILENAME = "allCountries.txt";
  private static final String ADM5_CODES_FILENAME = "adminCode5.txt";
  private static final String HIERARCHY_FILENAME = "hierarchy.txt";

  private static final String IPV4_PATTERN =
      "(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])";
  private static final Pattern VALID_IPV4_PATTERN =
      Pattern.compile(IPV4_PATTERN, Pattern.CASE_INSENSITIVE);

  public static final String OUT_FILENAME_LOCATIONS = "locations.tsv";
  public static final String OUT_FILENAME_ALTNAMES = "altNames.tsv";
  public static final String OUT_FILENAME_LINKS = "links.tsv";

  public static final Character FIELD_DELIM = '\t';
  public static final String FIELD_DELIM_STR = FIELD_DELIM + "";

  public static final String PREFIX_PARENT = "PARENT_";
  public static final String PREFIX_CHILD = "CHILD_";
  public static final String SUFFIX_CODE_COL = "_CODE";
  public static final String SUFFIX_GEONAMES_COL = "_GEONAMES_URL";
  public static final String SUFFIX_STATE_ID_COL = "_STATE_ID";

  public static final String FEAT_CLASS_ADM = "A";
  public static final String FEAT_CODE_ADM1 = "ADM1";
  public static final String FEAT_CODE_ADM2 = "ADM2";
  public static final String FEAT_CODE_ADM3 = "ADM3";
  public static final String FEAT_CODE_ADM4 = "ADM4";
  public static final String FEAT_CODE_ADM5 = "ADM5";
  public static final String FEAT_CODE_ERROR = "*ERROR*";
  public static final String PARENT_LOCATIONS_FILTER = "PARENTS";

  public static final String LANG_DEFAULT = "default";
  public static final String LANG_ABBR = "abbr";
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
  public static final String COL_STATE_ID = "STATE_ID";
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

  // this field doesn't actually exists in allCountries.txt but it's in adminCode5.txt
  public static final String ADM5_COL_ADM5 = "ADM5";

  private static String[] getAdm5CodesInFieldNames() {
    return new String[] {//
        COL_GEONAMES_ID, //
        ADM5_COL_ADM5//
    };
  }

  private static String[] getAllCountriesInFieldNames() {
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

  public static String[] getAllCountriesWithAdm5InFieldNames() {
    return joinArrays(getAllCountriesInFieldNames(), new String[] {ADM5_COL_ADM5});
  }

  public static String[] getAllCountiresExtraCols() {
    return new String[] {//
        COL_GEONAMES_URL, //
        COL_STATE_ID, //
        ALL_COUNTRIES_COL_ADM1 + SUFFIX_CODE_COL, //
        ALL_COUNTRIES_COL_ADM1 + SUFFIX_GEONAMES_COL, //
        ALL_COUNTRIES_COL_ADM1 + SUFFIX_STATE_ID_COL, //
        ALL_COUNTRIES_COL_ADM2 + SUFFIX_CODE_COL, //
        ALL_COUNTRIES_COL_ADM2 + SUFFIX_GEONAMES_COL, //
        ALL_COUNTRIES_COL_ADM2 + SUFFIX_STATE_ID_COL, //
        ALL_COUNTRIES_COL_ADM3 + SUFFIX_CODE_COL, //
        ALL_COUNTRIES_COL_ADM3 + SUFFIX_GEONAMES_COL, //
        ALL_COUNTRIES_COL_ADM3 + SUFFIX_STATE_ID_COL, //
        ALL_COUNTRIES_COL_ADM4 + SUFFIX_CODE_COL, //
        ALL_COUNTRIES_COL_ADM4 + SUFFIX_GEONAMES_COL, //
        ALL_COUNTRIES_COL_ADM4 + SUFFIX_STATE_ID_COL, //
        ADM5_COL_ADM5 + SUFFIX_CODE_COL, //
        ADM5_COL_ADM5 + SUFFIX_GEONAMES_COL, //
        ADM5_COL_ADM5 + SUFFIX_STATE_ID_COL, //
    };
  }

  public static String[] getAllCountriesOutFieldNames() {
    return joinArrays(getAllCountriesWithAdm5InFieldNames(), getAllCountiresExtraCols());
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
    locationColumns.add(COL_GEONAMES_ID);
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


  public static final String HIERARCHY_COL_PARENT = "PARENT_ID";
  public static final String HIERARCHY_COL_CHILD = "CHILD_ID";
  public static final String HIERARCHY_COL_TYPE = "REL_TYPE";

  public static String[] getHierarchyInFieldNames() {
    return new String[] {//
        HIERARCHY_COL_PARENT, //
        HIERARCHY_COL_CHILD, //
        HIERARCHY_COL_TYPE //
    };
  }

  public static String[] getHierarchyExtraFields() {
    return new String[] {//
        PREFIX_PARENT + SUFFIX_GEONAMES_COL, //
        PREFIX_PARENT + SUFFIX_STATE_ID_COL, //
        PREFIX_CHILD + SUFFIX_GEONAMES_COL, //
        PREFIX_CHILD + SUFFIX_STATE_ID_COL//
    };
  }

  public static Map<String, String[]> getFiltersMap(String[] featuresFilter) {
    final Map<String, String[]> ret = new HashMap<>();
    ret.put(PARENT_LOCATIONS_FILTER, new String[0]);
    for (int i = 0; i < featuresFilter.length; i++) {
      final String[] propKeyValue = featuresFilter[i].split(Pattern.quote("="));
      ret.put(featuresFilter[i], propKeyValue[1].split(Pattern.quote(",")));
    }
    return ret;
  }

  public static Map<String, String> getOutFileNamesMap(String outDir, String country,
      Map<String, String[]> filtersMap) {
    final Map<String, String> ret = new HashMap<>();
    for (String filterStr : filtersMap.keySet()) {
      final String[] propKeyValue = filterStr.split(Pattern.quote("="));
      ret.put(filterStr, "file:" + java.nio.file.Paths.get(outDir, country, propKeyValue[0]) + "-");
    }
    return ret;
  }

  public static String getGeonamesUrl(String geonamesId) {
    return "http://www.geonames.org/" + geonamesId;
  }

  public static String getWikidataUrl(String wikiDataEntityId) {
    return "http://www.wikidata.org/entity/" + wikiDataEntityId;
  }

  public static String getStateId(final String geonamesId, final String lastUpdate) {
    return "st/" + geonamesId + "/" + lastUpdate.replaceAll("\\D", "");
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

  // Dataset reading ----------
  public static DataSet<Row> readAllCountries(ExecutionEnvironment env, String... projectedFields) {
    final Path allCountriesPath = new Path(BASE_DIR, ALL_COUNTRIES_FILENAME);
    final String[] inFields = getAllCountriesInFieldNames();
    final String allCountriesInHeader = String.join(FIELD_DELIM_STR, inFields);
    DataSet<Row> rows = env//
        .readFile(new TextInputFormat(allCountriesPath), allCountriesPath.toString())//
        .flatMap(new CsvLine2Row(allCountriesInHeader, false, FIELD_DELIM, null))//
        .returns(new RowTypeInfo(FlinkUtils.getDefaultFlinkFieldTypes(inFields)));
    // add adm5 to original dataset
    final String[] outFn = getAllCountriesWithAdm5InFieldNames();
    final Map<String, Integer> allCountriesFpos = getFieldPosMap(inFields);
    final Map<String, Integer> adm5Fpos = getFieldPosMap(getAdm5CodesInFieldNames());
    final Map<String, Integer> outFpos = getFieldPosMap(outFn);

    // join only if necessary
    if (containsField(projectedFields, ADM5_COL_ADM5)) {
      final DataSet<Row> adm5CodeRows = readAdm5(env);
      rows = rows.leftOuterJoin(adm5CodeRows)//
          .where(r -> (String) r.getField(allCountriesFpos.get(COL_GEONAMES_ID)))
          .equalTo(r -> (String) r.getField(adm5Fpos.get(COL_GEONAMES_ID)))
          .with(new JoinFunction<Row, Row, Row>() {
            private static final long serialVersionUID = 1L;
            private final int adm5outPos = outFpos.get(ADM5_COL_ADM5);
            private final int adm5inPos = adm5Fpos.get(ADM5_COL_ADM5);
            private final Row reuse = new Row(outFn.length);

            @Override
            public Row join(Row left, Row rigth) throws Exception {
              for (Entry<String, Integer> entry : allCountriesFpos.entrySet()) {
                reuse.setField(outFpos.get(entry.getKey()), left.getField(entry.getValue()));
              }
              reuse.setField(adm5outPos, rigth == null ? null : rigth.getField(adm5inPos));
              return reuse;
            }
          }).returns(new RowTypeInfo(FlinkUtils.getDefaultFlinkFieldTypes(outFn)));
    } else {
      rows = rows.map(new RowEnlarger(outFn.length))
          .returns(new RowTypeInfo(FlinkUtils.getDefaultFlinkFieldTypes(outFn)));
    }
    if (projectedFields == null || projectedFields.length == 0) {
      return rows;
    }
    return rows.map(new RowFieldsProjector(projectedFields, outFn))//
        .returns(new RowTypeInfo(FlinkUtils.getDefaultFlinkFieldTypes(projectedFields)));
  }

  private static boolean containsField(String[] projectedFields, String targetField) {
    if (projectedFields != null && projectedFields.length > 0) {
      for (String fieldName : projectedFields) {
        if (fieldName.equals(targetField)) {
          return true;
        }
      }
    }
    return false;
  }

  public static DataSet<Row> getAltNamesRows(ExecutionEnvironment env, String[] langsToFilter) {
    final Path altNamesPath = new Path(BASE_DIR, ALT_NAMES_FILENAME);
    final String[] altNamesInFn = getAltNamesInFieldNames();
    final String altNamesInHeader = String.join(FIELD_DELIM_STR, altNamesInFn);
    final RowTypeInfo rowType = new RowTypeInfo(FlinkUtils.getDefaultFlinkFieldTypes(altNamesInFn));
    return env.readFile(new TextInputFormat(altNamesPath), altNamesPath.toString())
        .flatMap(new CsvLine2Row(altNamesInHeader, false, FIELD_DELIM, null))//
        .returns(rowType)//
        .flatMap(new AltNamesSelector(langsToFilter, GeoNamesUtils.getFieldPosMap(altNamesInFn)))
        .returns(rowType);
  }

  public static DataSet<Row> getHierarchyRows(final ExecutionEnvironment env) {
    final Path hierarchyPath = new Path(BASE_DIR, HIERARCHY_FILENAME);
    final String[] hierarchyInFn = getHierarchyInFieldNames();
    final String altNamesInHeader = String.join(FIELD_DELIM_STR, hierarchyInFn);
    return env.readFile(new TextInputFormat(hierarchyPath), hierarchyPath.toString())
        .flatMap(new CsvLine2Row(altNamesInHeader, false, FIELD_DELIM, null))//
        .returns(new RowTypeInfo(FlinkUtils.getDefaultFlinkFieldTypes(hierarchyInFn)));
  }

  private static DataSet<Row> readAdm5(ExecutionEnvironment env) {
    final Path adm5FilePath = new Path(BASE_DIR, ADM5_CODES_FILENAME);
    final String[] inFields = getAdm5CodesInFieldNames();
    final String adm5InHeader = String.join(FIELD_DELIM_STR, inFields);
    return env.readFile(new TextInputFormat(adm5FilePath), adm5FilePath.toString())//
        .flatMap(new CsvLine2Row(adm5InHeader, false, FIELD_DELIM, null))//
        .returns(new RowTypeInfo(FlinkUtils.getDefaultFlinkFieldTypes(inFields)));
  }

  public static FlatMapOperator<Row, AdmCodeTuple> readAdmCodes(final DataSet<Row> allCountriesRows,
      String admCode) {
    return allCountriesRows
        .flatMap(new AdmCodeToGeonamesIdGenerator(getAllCountriesWithAdm5InFieldNames(), admCode));
  }
}
