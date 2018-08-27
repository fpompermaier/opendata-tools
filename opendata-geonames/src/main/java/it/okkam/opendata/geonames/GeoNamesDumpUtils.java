package it.okkam.opendata.geonames;

import static it.okkam.opendata.geonames.GeoNamesConstants.ALL_COUNTRIES_COL_FEATURE_CLASS;
import static it.okkam.opendata.geonames.GeoNamesConstants.ALL_COUNTRIES_COL_FEATURE_CODE;
import static it.okkam.opendata.geonames.GeoNamesConstants.ALL_COUNTRIES_COL_GEONAMES_ID;
import static it.okkam.opendata.geonames.GeoNamesConstants.ALL_COUNTRIES_COL_GEONAMES_URL;
import static it.okkam.opendata.geonames.GeoNamesConstants.ALL_COUNTRIES_COL_LAST_UPDATE;
import static it.okkam.opendata.geonames.GeoNamesConstants.ALL_COUNTRIES_COL_LAT;
import static it.okkam.opendata.geonames.GeoNamesConstants.ALL_COUNTRIES_COL_LON;
import static it.okkam.opendata.geonames.GeoNamesConstants.ALL_COUNTRIES_COL_NAME;
import static it.okkam.opendata.geonames.GeoNamesConstants.ALL_COUNTRIES_COL_NAME_ASCII;
import static it.okkam.opendata.geonames.GeoNamesConstants.ALL_COUNTRIES_COL_POPULATION;
import static it.okkam.opendata.geonames.GeoNamesConstants.ALTNAMES_COL_GEONAMES_ID;
import static it.okkam.opendata.geonames.GeoNamesConstants.ALTNAMES_COL_GEONAMES_URL;
import static it.okkam.opendata.geonames.GeoNamesConstants.FIELD_DELIM;
import static it.okkam.opendata.geonames.GeoNamesConstants.FIELD_DELIM_STR;
import static it.okkam.opendata.geonames.GeoNamesConstants.LANG_DE;
import static it.okkam.opendata.geonames.GeoNamesConstants.LANG_DEFAULT;
import static it.okkam.opendata.geonames.GeoNamesConstants.LANG_EN;
import static it.okkam.opendata.geonames.GeoNamesConstants.LANG_ES;
import static it.okkam.opendata.geonames.GeoNamesConstants.LANG_FR;
import static it.okkam.opendata.geonames.GeoNamesConstants.LANG_IT;
import static it.okkam.opendata.geonames.GeoNamesConstants.LANG_LINK;
import static it.okkam.opendata.geonames.GeoNamesConstants.LANG_POSTCODE;
import static it.okkam.opendata.geonames.GeoNamesConstants.LANG_PT;
import static it.okkam.opendata.geonames.GeoNamesConstants.LANG_RU;
import static it.okkam.opendata.geonames.GeoNamesConstants.LANG_WIKIDATA;
import static it.okkam.opendata.geonames.GeoNamesConstants.SEPARATOR_PATTERN;
import static it.okkam.opendata.geonames.GeoNamesConstants.getAllCountriesInFieldNames;
import static it.okkam.opendata.geonames.GeoNamesConstants.getAltNamesInFieldNames;
import static it.okkam.opendata.geonames.GeoNamesConstants.getFieldPosMap;

import it.okkam.flink.csv.CsvLine2Row;
import it.okkam.opendata.geonames.flink.AllCountriesAltNamesJoiner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;

public class GeoNamesDumpUtils {

  private static final String FLINK_TMP_DIR = "/tmp";
  private static final String BASE_DIR = "file:/media/okkam/FLASSD/datalinks/datasets/geonames/";
  private static final String ALL_COUNTRIES_FILENAME = "allCountries.txt";
  private static final String ALT_NAMES_FILENAME = "alternateNamesV2.txt";
  private static final String OUT_DIR = "/tmp/geonames/";

  // GEONAMES_URL,GEONAMES_ID,NAME,NAME_ASCII,LAT,LON,FEATURE_CLASS,FEATURE_CODE,POPULATION,LAST_UPDATE
  private static final String ALL_COUNTRIES_OUT_COLS = new StringBuilder()//
      .append(ALL_COUNTRIES_COL_GEONAMES_URL).append(FIELD_DELIM) //
      .append(ALL_COUNTRIES_COL_GEONAMES_ID).append(FIELD_DELIM) //
      .append(ALL_COUNTRIES_COL_NAME).append(FIELD_DELIM) //
      .append(ALL_COUNTRIES_COL_NAME_ASCII).append(FIELD_DELIM) //
      .append(ALL_COUNTRIES_COL_LAT).append(FIELD_DELIM) //
      .append(ALL_COUNTRIES_COL_LON).append(FIELD_DELIM) //
      .append(ALL_COUNTRIES_COL_FEATURE_CLASS).append(FIELD_DELIM) //
      .append(ALL_COUNTRIES_COL_FEATURE_CODE).append(FIELD_DELIM) //
      .append(ALL_COUNTRIES_COL_POPULATION).append(FIELD_DELIM) //
      .append(ALL_COUNTRIES_COL_LAST_UPDATE)//
      .toString();

  private static final String[] FEATURES_FILTER = new String[] {//
      "L|CONT", // all continents
      "IT|A|*", // Italian Administrative divisions
      "IT|P|*",// Italian populated places
  };

  private static final String[] ALT_NAMES_FILTER = new String[] {//
      LANG_DEFAULT, //
      LANG_IT, //
      LANG_EN, //
      LANG_DE, //
      LANG_FR, //
      LANG_ES, //
      LANG_RU, //
      LANG_PT, //
      LANG_LINK, //
      LANG_WIKIDATA, //
      LANG_POSTCODE//
  };

  public static void main(String[] args) throws Exception {
    final Map<String, String[]> filtersMap = getFiltersMap();
    final Map<String, String> outFilesMap = getOutFileNamesMap(filtersMap);

    final ExecutionEnvironment env = getExecutionEnv();
    final BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

    final String[] altNamesOutFn = getAltNamesOutFieldNames();
    final String[] outFn = getAllCountriesOutFieldNames(altNamesOutFn);
    final RowTypeInfo outRowType = new RowTypeInfo(getFlinkFieldTypes(outFn));
    final TupleTypeInfo<Tuple2<String, Row>> tupleTypeInfo =
        new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO, outRowType);

    final DataSet<Tuple2<String, Row>> allCountriesRows = getAllCountries(env, filtersMap, outFn);
    final DataSet<Row> altNamesRows = getAltNamesRows(env, altNamesOutFn);

    final DataSet<Tuple2<String, Row>> joined = allCountriesRows.leftOuterJoin(altNamesRows)//
        .where(t -> (String) t.f1.getField(0)).equalTo(r -> (String) r.getField(0))//
        .with(new AllCountriesAltNamesJoiner(outFn, altNamesOutFn))//
        .returns(tupleTypeInfo);

    for (Entry<String, String> filterEntry : outFilesMap.entrySet()) {
      final String filterKey = filterEntry.getKey();
      final DataSet<Row> outDs = joined.filter(t -> t.f0.equals(filterKey))//
          .map(t -> t.f1).returns(outRowType);
      tableEnv.fromDataSet(outDs)//
          .writeToSink(new CsvTableSink(outFilesMap.get(filterEntry.getKey()), FIELD_DELIM_STR, 1,
              WriteMode.OVERWRITE));
    }

    env.execute("Geonames dump transformer");

  }

  private static String[] getAltNamesOutFieldNames() {
    final String[] ret = new String[1 + ALT_NAMES_FILTER.length];
    ret[0] = ALTNAMES_COL_GEONAMES_URL;
    for (int i = 0; i < ALT_NAMES_FILTER.length; i++) {
      ret[i + 1] = ALT_NAMES_FILTER[i].toUpperCase();
    }
    return ret;
  }

  private static String[] getAllCountriesOutFieldNames(String[] altNamesOutFn) {
    final String[] allCountriesOutFn = ALL_COUNTRIES_OUT_COLS.split(Pattern.quote(FIELD_DELIM_STR));
    final String[] altNamesWithoutGeonamesUrl =
        Arrays.copyOfRange(altNamesOutFn, 1, altNamesOutFn.length);
    return joinArrays(allCountriesOutFn, altNamesWithoutGeonamesUrl);
  }

  private static DataSet<Row> getAltNamesRows(final ExecutionEnvironment env,
      final String[] altNamesOutFn) {
    final Path altNamesPath = new Path(BASE_DIR, ALT_NAMES_FILENAME);
    final String[] altNamesInFn = getAltNamesInFieldNames();
    final String altNamesInHeader = String.join(FIELD_DELIM_STR, altNamesInFn);
    final Map<String, Integer> altNamesInFpos = getFieldPosMap(altNamesInFn);

    final DataSet<String> lines =
        env.readFile(new TextInputFormat(altNamesPath), altNamesPath.toString());
    final DataSet<Row> altNamesRows =
        lines.flatMap(new CsvLine2Row(altNamesInHeader, false, FIELD_DELIM, null))//
            .returns(new RowTypeInfo(getFlinkFieldTypes(altNamesInFn)));

    return altNamesRows//
        .groupBy(r -> r.getField(altNamesInFpos.get(ALTNAMES_COL_GEONAMES_ID)).toString())
        .reduceGroup(new AltNamesGenerator(ALT_NAMES_FILTER, altNamesOutFn, altNamesInFpos))//
        .returns(new RowTypeInfo(getFlinkFieldTypes(altNamesOutFn)));
  }

  private static DataSet<Tuple2<String, Row>> getAllCountries(final ExecutionEnvironment env,
      final Map<String, String[]> filtersMap, String[] allCountriesOutFn) {
    final Path allCountriesPath = new Path(BASE_DIR, ALL_COUNTRIES_FILENAME);
    final String[] allCountriesInFn = getAllCountriesInFieldNames();
    final String allCountriesInHeader = String.join(FIELD_DELIM_STR, allCountriesInFn);
    final Map<String, Integer> allCountriesInFpos = getFieldPosMap(allCountriesInFn);

    final DataSet<String> lines =
        env.readFile(new TextInputFormat(allCountriesPath), allCountriesPath.toString());
    final DataSet<Row> allCountriesRows =
        lines.flatMap(new CsvLine2Row(allCountriesInHeader, false, FIELD_DELIM, null))//
            .returns(new RowTypeInfo(getFlinkFieldTypes(allCountriesInFn)));

    return allCountriesRows
        .flatMap(new AllCountriesGenerator(allCountriesInFpos, allCountriesOutFn, filtersMap))//
        .returns(new TupleTypeInfo<Tuple2<String, Row>>(BasicTypeInfo.STRING_TYPE_INFO,
            new RowTypeInfo(getFlinkFieldTypes(allCountriesOutFn))));
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

  // read everything as string
  private static TypeInformation<?>[] getFlinkFieldTypes(String[] fieldNames) {
    final TypeInformation<?>[] ret = new TypeInformation[fieldNames.length];
    for (int i = 0; i < ret.length; i++) {
      ret[i] = BasicTypeInfo.STRING_TYPE_INFO;
    }
    return ret;
  }

  /**
   * Returns a pre-configured Execution env suitable for local tests from an IDE.
   * 
   * @return a suitable ExecutionEnvironment for local test from an IDE
   */
  public static ExecutionEnvironment getExecutionEnv() {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    if (env instanceof LocalEnvironment) {
      Configuration conf = new Configuration();
      conf.setString(CoreOptions.TMP_DIRS, FLINK_TMP_DIR);
      conf.setString(BlobServerOptions.STORAGE_DIRECTORY, FLINK_TMP_DIR);
      // conf.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 4); //NOSONAR
      // conf.setFloat(ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY, 0.4f);//NOSONAR
      // conf.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, 32768 * 2);//NOSONAR
      conf.setLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT, 32768 * 2);// NOSONAR
      env = ExecutionEnvironment.createLocalEnvironment(conf);
      env.setParallelism(Runtime.getRuntime().availableProcessors());
    }
    // env.getConfig().disableGenericTypes();//NOSONAR
    return env;
  }

  private static Map<String, String> getOutFileNamesMap(Map<String, String[]> filtersMap) {
    final Map<String, String> ret = new HashMap<>();
    for (String filterStr : filtersMap.keySet()) {
      final String[] tokens = filterStr.split(SEPARATOR_PATTERN);
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].equals("*") ? "ALL" : tokens[i];
      }
      ret.put(filterStr, "file:" + java.nio.file.Paths.get(OUT_DIR, tokens));
    }
    return ret;
  }

  private static Map<String, String[]> getFiltersMap() {
    final Map<String, String[]> ret = new HashMap<>();
    for (int i = 0; i < FEATURES_FILTER.length; i++) {
      ret.put(FEATURES_FILTER[i], FEATURES_FILTER[i].split(SEPARATOR_PATTERN));
    }
    return ret;
  }

}
