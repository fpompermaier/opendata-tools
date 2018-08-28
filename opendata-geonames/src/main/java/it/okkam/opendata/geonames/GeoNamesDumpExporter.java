package it.okkam.opendata.geonames;

import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_FEATURE_CLASS;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_FEATURE_CODE;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_LAST_UPDATE;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_LAT;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_LON;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_NAME;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_NAME_ASCII;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_POPULATION;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALTNAMES_COL_LANG;
import static it.okkam.opendata.geonames.GeoNamesUtils.COL_GEONAMES_ID;
import static it.okkam.opendata.geonames.GeoNamesUtils.COL_GEONAMES_URL;
import static it.okkam.opendata.geonames.GeoNamesUtils.FIELD_DELIM;
import static it.okkam.opendata.geonames.GeoNamesUtils.FIELD_DELIM_STR;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_DE;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_DEFAULT;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_EN;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_ES;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_FR;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_IT;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_LINK;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_POSTCODE;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_PT;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_RU;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_WIKIDATA;
import static it.okkam.opendata.geonames.GeoNamesUtils.LINKS_COL_DOMAIN;
import static it.okkam.opendata.geonames.GeoNamesUtils.LINKS_COL_LANG;
import static it.okkam.opendata.geonames.GeoNamesUtils.OUT_FILENAME_ALTNAMES;
import static it.okkam.opendata.geonames.GeoNamesUtils.OUT_FILENAME_LINKS;
import static it.okkam.opendata.geonames.GeoNamesUtils.OUT_FILENAME_LOCATIONS;
import static it.okkam.opendata.geonames.GeoNamesUtils.getAllCountriesInFieldNames;
import static it.okkam.opendata.geonames.GeoNamesUtils.getAltNamesEnrichedFieldNames;
import static it.okkam.opendata.geonames.GeoNamesUtils.getAltNamesInFieldNames;
import static it.okkam.opendata.geonames.GeoNamesUtils.getFieldPosMap;
import static it.okkam.opendata.geonames.GeoNamesUtils.getFiltersMap;
import static it.okkam.opendata.geonames.GeoNamesUtils.getOutFileNamesMap;
import static it.okkam.opendata.geonames.GeoNamesUtils.joinArrays;

import it.okkam.flink.csv.CsvLine2Row;
import it.okkam.opendata.geonames.flink.AllCountriesAltNamesJoiner;
import it.okkam.opendata.geonames.flink.AllCountriesGenerator;
import it.okkam.opendata.geonames.flink.AltNamesGenerator;
import it.okkam.opendata.geonames.flink.LinkRowGenerator;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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

public class GeoNamesDumpExporter {

  private static final String FLINK_TMP_DIR = "/tmp";
  private static final String BASE_DIR = "file:/tmp/datasets/geonames/";
  private static final String ALL_COUNTRIES_FILENAME = "allCountries.txt";
  private static final String ALT_NAMES_FILENAME = "alternateNamesV2.txt";
  private static final String OUT_DIR = "/tmp/geonames/";

  // GEONAMES_URL,GEONAMES_ID,NAME,NAME_ASCII,LAT,LON,FEATURE_CLASS,FEATURE_CODE,POPULATION,LAST_UPDATE
  private static final String[] ALL_COUNTRIES_OUT_COLS = new String[] {//
      COL_GEONAMES_URL, //
      COL_GEONAMES_ID, //
      ALL_COUNTRIES_COL_NAME, //
      ALL_COUNTRIES_COL_NAME_ASCII, //
      ALL_COUNTRIES_COL_LAT, //
      ALL_COUNTRIES_COL_LON, //
      ALL_COUNTRIES_COL_FEATURE_CLASS, //
      ALL_COUNTRIES_COL_FEATURE_CODE, //
      ALL_COUNTRIES_COL_POPULATION, //
      ALL_COUNTRIES_COL_LAST_UPDATE, //
  };

  private static final String[] FEATURES_FILTER = new String[] {//
      "L|CONT", // all continents
      "IT|A|*", // Italian Administrative divisions
      "IT|P|*",// Italian populated places
  };

  private static final String[] ALTNAMES_LANG_FILTER = new String[] {//
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
    final Map<String, String[]> filtersMap = getFiltersMap(FEATURES_FILTER);
    final Map<String, String> outFilesMap = getOutFileNamesMap(OUT_DIR, filtersMap);

    final ExecutionEnvironment env = getExecutionEnv();
    final BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);


    // main locations file
    final String[] locationsOutFn = getLocationsOutFieldNames();
    final DataSet<Tuple2<String, Row>> locations = getLocations(env, filtersMap, locationsOutFn);
    writeLocationsFile(outFilesMap, tableEnv, locationsOutFn, locations);

    // alternatives transformation -> links + alternative names
    final String[] altNamesInFn = getAltNamesFieldNames();
    final DataSet<Row> altNames = getAltNamesRows(env, altNamesInFn);
    final List<List<String>> altNamesEnrichedFn = getAltNamesEnrichedFieldNames();
    final String[] leftFields = altNamesEnrichedFn.get(0).toArray(new String[0]);
    final String[] rightFields = altNamesEnrichedFn.get(1).toArray(new String[0]);
    final String[] altNamesEnrichedOutFn = GeoNamesUtils.joinArrays(leftFields, rightFields);
    final Map<String, Integer> altNamesEnrichedFieldPos = //
        GeoNamesUtils.getFieldPosMap(altNamesEnrichedOutFn);
    final DataSet<Tuple2<String, Row>> altNamesEnriched = locations.join(altNames)//
        .where(t -> (String) t.f1.getField(getGeonameIdFieldPos(locationsOutFn)))//
        .equalTo(r -> (String) r.getField(getGeonameIdFieldPos(altNamesInFn)))//
        .with(new AllCountriesAltNamesJoiner(leftFields, rightFields, altNamesEnrichedFieldPos,
            getFieldPosMap(locationsOutFn), getFieldPosMap(altNamesInFn)))//
        .returns(new TupleTypeInfo<>(BasicTypeInfo.STRING_TYPE_INFO,
            new RowTypeInfo(getFlinkFieldTypes(altNamesEnrichedOutFn))));
    writeLangAndLinkFiles(outFilesMap, tableEnv, locationsOutFn, altNamesEnrichedFieldPos,
        altNamesEnriched);

    env.execute("Geonames dump transformer");

  }

  private static void writeLangAndLinkFiles(final Map<String, String> outFilesMap,
      final BatchTableEnvironment tableEnv, final String[] locationsOutFn,
      final Map<String, Integer> altNamesEnrichedFieldPos,
      final DataSet<Tuple2<String, Row>> altNamesEnriched) {
    final Integer langTypePos = altNamesEnrichedFieldPos.get(ALTNAMES_COL_LANG);
    for (Entry<String, String> filterEntry : outFilesMap.entrySet()) {
      final String filterKey = filterEntry.getKey();
      final DataSet<Row> outDs = altNamesEnriched.filter(t -> t.f0.equals(filterKey))//
          .map(t -> t.f1).returns(new RowTypeInfo(getFlinkFieldTypes(locationsOutFn)));
      final DataSet<Row> altNamesDs = outDs.filter(row -> !isLink(row, langTypePos));
      final DataSet<Row> linkDs = outDs.filter(row -> isLink(row, langTypePos));

      // alternative names
      final String altNamesFile = outFilesMap.get(filterEntry.getKey()) + OUT_FILENAME_ALTNAMES;
      tableEnv.fromDataSet(altNamesDs)//
          .writeToSink(new CsvTableSink(altNamesFile, FIELD_DELIM_STR, 1, WriteMode.OVERWRITE));


      // links
      final String linkFile = outFilesMap.get(filterEntry.getKey()) + OUT_FILENAME_LINKS;
      final String[] extraLinkFields = new String[] {LINKS_COL_DOMAIN, LINKS_COL_LANG};
      final String[] linksOutFn = joinArrays(locationsOutFn, extraLinkFields);
      final DataSet<Row> enrichedLinkDs =
          linkDs.map(new LinkRowGenerator(altNamesEnrichedFieldPos, linksOutFn))
              .returns(new RowTypeInfo(getFlinkFieldTypes(linksOutFn)));
      tableEnv.fromDataSet(enrichedLinkDs)//
          .writeToSink(new CsvTableSink(linkFile, FIELD_DELIM_STR, 1, WriteMode.OVERWRITE));
    }
  }

  private static void writeLocationsFile(final Map<String, String> outFilesMap,
      final BatchTableEnvironment tableEnv, final String[] locationsOutFn,
      final DataSet<Tuple2<String, Row>> locations) {
    for (Entry<String, String> filterEntry : outFilesMap.entrySet()) {
      final String filterKey = filterEntry.getKey();
      final DataSet<Row> outDs = locations.filter(t -> t.f0.equals(filterKey))//
          .map(t -> t.f1).returns(new RowTypeInfo(getFlinkFieldTypes(locationsOutFn)));
      final String locFile = outFilesMap.get(filterEntry.getKey()) + OUT_FILENAME_LOCATIONS;
      tableEnv.fromDataSet(outDs)//
          .writeToSink(new CsvTableSink(locFile, FIELD_DELIM_STR, 1, WriteMode.OVERWRITE));
    }
  }

  private static boolean isLink(Row row, Integer langTypePos) {
    return row.getField(langTypePos) != null && LANG_LINK.equals(row.getField(langTypePos));
  }

  private static int getGeonameIdFieldPos(String[] fieldNames) {
    for (int i = 0; i < fieldNames.length; i++) {
      if (fieldNames[i].equals(COL_GEONAMES_ID)) {
        return i;
      }
    }
    return -1;
  }

  private static String[] getAltNamesFieldNames() {
    final String[] fieldNames = getAltNamesInFieldNames();
    final String[] ret = new String[fieldNames.length];
    for (int i = 0; i < fieldNames.length; i++) {
      ret[i] = fieldNames[i].toUpperCase();
    }
    return ret;
  }

  private static String[] getLocationsOutFieldNames() {
    return ALL_COUNTRIES_OUT_COLS;
  }

  private static DataSet<Row> getAltNamesRows(final ExecutionEnvironment env,
      final String[] altNamesInFn) {
    final Path altNamesPath = new Path(BASE_DIR, ALT_NAMES_FILENAME);
    final Map<String, Integer> altNamesInFpos = getFieldPosMap(altNamesInFn);
    final RowTypeInfo rowType = new RowTypeInfo(getFlinkFieldTypes(altNamesInFn));
    final String altNamesInHeader = String.join(FIELD_DELIM_STR, altNamesInFn);

    return env.readFile(new TextInputFormat(altNamesPath), altNamesPath.toString())
        .flatMap(new CsvLine2Row(altNamesInHeader, false, FIELD_DELIM, null))//
        .returns(rowType)//
        .flatMap(new AltNamesGenerator(ALTNAMES_LANG_FILTER, altNamesInFn, altNamesInFpos))//
        .returns(rowType);
  }

  private static DataSet<Tuple2<String, Row>> getLocations(final ExecutionEnvironment env,
      final Map<String, String[]> filtersMap, String[] allCountriesOutFn) {
    final Path allCountriesPath = new Path(BASE_DIR, ALL_COUNTRIES_FILENAME);
    final String[] allCountriesInFn = getAllCountriesInFieldNames();
    final String allCountriesInHeader = String.join(FIELD_DELIM_STR, allCountriesInFn);
    final Map<String, Integer> allCountriesInFpos = getFieldPosMap(allCountriesInFn);

    final DataSet<Row> allCountriesRows = env//
        .readFile(new TextInputFormat(allCountriesPath), allCountriesPath.toString())//
        .flatMap(new CsvLine2Row(allCountriesInHeader, false, FIELD_DELIM, null))//
        .returns(new RowTypeInfo(getFlinkFieldTypes(allCountriesInFn)));

    return allCountriesRows
        .flatMap(new AllCountriesGenerator(allCountriesInFpos, allCountriesOutFn, filtersMap))//
        .returns(new TupleTypeInfo<Tuple2<String, Row>>(BasicTypeInfo.STRING_TYPE_INFO,
            new RowTypeInfo(getFlinkFieldTypes(allCountriesOutFn))));
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

}
