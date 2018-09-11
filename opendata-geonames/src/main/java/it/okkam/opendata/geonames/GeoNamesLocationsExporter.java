package it.okkam.opendata.geonames;

import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_ADM1;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_ADM2;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_ADM3;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_ADM4;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_FILENAME;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_OUT_EXTRA_COLS;
import static it.okkam.opendata.geonames.GeoNamesUtils.BASE_DIR;
import static it.okkam.opendata.geonames.GeoNamesUtils.FEAT_CODE_ADM1;
import static it.okkam.opendata.geonames.GeoNamesUtils.FEAT_CODE_ADM2;
import static it.okkam.opendata.geonames.GeoNamesUtils.FEAT_CODE_ADM4;
import static it.okkam.opendata.geonames.GeoNamesUtils.FEAT_CODE_ERROR;
import static it.okkam.opendata.geonames.GeoNamesUtils.FIELD_DELIM;
import static it.okkam.opendata.geonames.GeoNamesUtils.FIELD_DELIM_STR;
import static it.okkam.opendata.geonames.GeoNamesUtils.OUT_DIR;
import static it.okkam.opendata.geonames.GeoNamesUtils.OUT_FILENAME_LOCATIONS;
import static it.okkam.opendata.geonames.GeoNamesUtils.SUFFIX_CODE_COL;
import static it.okkam.opendata.geonames.GeoNamesUtils.SUFFIX_GEONAMES_COL;
import static it.okkam.opendata.geonames.GeoNamesUtils.getAllCountriesInFieldNames;
import static it.okkam.opendata.geonames.GeoNamesUtils.getFieldPosMap;
import static it.okkam.opendata.geonames.GeoNamesUtils.getFiltersMap;
import static it.okkam.opendata.geonames.GeoNamesUtils.getOutFileNamesMap;

import it.okkam.flink.csv.CsvLine2Row;
import it.okkam.opendata.geonames.flink.AdmCodeJoiner;
import it.okkam.opendata.geonames.flink.AdmCodeToGeonamesIdGenerator;
import it.okkam.opendata.geonames.flink.AllCountriesGenerator;
import it.okkam.opendata.geonames.flink.FlinkUtils;
import it.okkam.opendata.geonames.flink.model.AdmCodeTuple;
import it.okkam.opendata.geonames.flink.model.LabelledRow;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;

public class GeoNamesLocationsExporter {

  private static final String[] FEATURES_FILTER = new String[] {//
      // "L|CONT", // all continents// "L|CONT", // all continents
      "AD|A|*", // Italian administrative divisions
      // "IT|A|*", // Italian administrative divisions
      // "IT|P|*",// Italian populated places
      // "US|A|*", // American administrative divisions
      // "US|P|*", // American populated places
  };

  public static void main(String[] args) throws Exception {
    final Map<String, String[]> filtersMap = getFiltersMap(FEATURES_FILTER);
    final Map<String, String> outFilesMap = getOutFileNamesMap(OUT_DIR, filtersMap);

    final ExecutionEnvironment env = FlinkUtils.getExecutionEnv();
    final BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);


    // main locations file
    final DataSet<Row> allCountriesRows = readAllCountries(env);
    final String[] locationsOutFn = GeoNamesUtils.getLocationsOutFieldNames();
    DataSet<LabelledRow> locations = allCountriesRows
        .flatMap(new AllCountriesGenerator(ALL_COUNTRIES_OUT_EXTRA_COLS, filtersMap))//
        .returns(LabelledRow.getTypeInfo(locationsOutFn));

    final Map<String, Integer> locationsFpos = getFieldPosMap(locationsOutFn);
    final int adm1CodePos = locationsFpos.get(ALL_COUNTRIES_COL_ADM1 + SUFFIX_CODE_COL);
    final int adm2CodePos = locationsFpos.get(ALL_COUNTRIES_COL_ADM2 + SUFFIX_CODE_COL);
    final int adm3CodePos = locationsFpos.get(ALL_COUNTRIES_COL_ADM3 + SUFFIX_CODE_COL);
    final int adm4CodePos = locationsFpos.get(ALL_COUNTRIES_COL_ADM4 + SUFFIX_CODE_COL);
    final int adm1GeonamesUrlPos = locationsFpos.get(ALL_COUNTRIES_COL_ADM1 + SUFFIX_GEONAMES_COL);
    final int adm2GeonamesUrlPos = locationsFpos.get(ALL_COUNTRIES_COL_ADM2 + SUFFIX_GEONAMES_COL);
    final int adm3GeonamesUrlPos = locationsFpos.get(ALL_COUNTRIES_COL_ADM3 + SUFFIX_GEONAMES_COL);
    final int adm4GeonamesUrlPos = locationsFpos.get(ALL_COUNTRIES_COL_ADM4 + SUFFIX_GEONAMES_COL);

    final DataSet<AdmCodeTuple> adm1Codes = readAdmCodes(allCountriesRows, FEAT_CODE_ADM1);
    final DataSet<AdmCodeTuple> adm2Codes = readAdmCodes(allCountriesRows, FEAT_CODE_ADM2);
    final DataSet<AdmCodeTuple> adm3Codes = readAdmCodes(allCountriesRows, FEAT_CODE_ADM2);
    final DataSet<AdmCodeTuple> adm4Codes = readAdmCodes(allCountriesRows, FEAT_CODE_ADM4);
    // retrieve adm1 geonames url
    locations = locations.filter(t -> t.f1.getField(adm1CodePos) == null).union(//
        locations.filter(t -> t.f1.getField(adm1CodePos) != null) //
            .leftOuterJoin(adm1Codes.filter(t -> !FEAT_CODE_ERROR.equals(t.f0)))//
            .where(t -> (String) t.f1.getField(adm1CodePos)).equalTo(0)//
            .with(new AdmCodeJoiner(adm1GeonamesUrlPos))//
            .returns(LabelledRow.getTypeInfo(locationsOutFn)));
    // retrieve adm2 geonames url
    locations = locations.filter(t -> t.f1.getField(adm2CodePos) == null).union(//
        locations.filter(t -> t.f1.getField(adm2CodePos) != null) //
            .leftOuterJoin(adm2Codes.filter(t -> !FEAT_CODE_ERROR.equals(t.f0)))//
            .where(t -> (String) t.f1.getField(adm2CodePos)).equalTo(0)//
            .with(new AdmCodeJoiner(adm2GeonamesUrlPos))//
            .returns(LabelledRow.getTypeInfo(locationsOutFn)));
    // retrieve adm3 geonames url
    locations = locations.filter(t -> t.f1.getField(adm3CodePos) == null).union(//
        locations.filter(t -> t.f1.getField(adm3CodePos) != null) //
            .leftOuterJoin(adm3Codes.filter(t -> !FEAT_CODE_ERROR.equals(t.f0)))//
            .where(t -> (String) t.f1.getField(adm3CodePos)).equalTo(0)//
            .with(new AdmCodeJoiner(adm3GeonamesUrlPos))//
            .returns(LabelledRow.getTypeInfo(locationsOutFn)));
    // retrieve adm4 geonames url
    locations = locations.filter(t -> t.f1.getField(adm4CodePos) == null).union(//
        locations.filter(t -> t.f1.getField(adm4CodePos) != null) //
            .leftOuterJoin(adm4Codes.filter(t -> !FEAT_CODE_ERROR.equals(t.f0)))//
            .where(t -> (String) t.f1.getField(adm4CodePos)).equalTo(0)//
            .with(new AdmCodeJoiner(adm4GeonamesUrlPos))//
            .returns(LabelledRow.getTypeInfo(locationsOutFn)));
    writeLocationsFile(outFilesMap, tableEnv, locationsOutFn, locations);

    // final DataSet<Tuple2<String, String>> adminCodeErrors = //
    // adm1Codes.filter(t-> FEAT_CODE_ERROR.equals(t.f0))
    // .union(adm2Codes.filter(t-> FEAT_CODE_ERROR.equals(t.f0)))
    // .union(adm3Codes.filter(t-> FEAT_CODE_ERROR.equals(t.f0)))
    // .union(adm4Codes.filter(t-> FEAT_CODE_ERROR.equals(t.f0)));
    // adminCodeErrors.print();

    env.execute("Geonames dump transformer");

  }

  private static FlatMapOperator<Row, AdmCodeTuple> readAdmCodes(
      final DataSet<Row> allCountriesRows, String admCode) {
    return allCountriesRows
        .flatMap(new AdmCodeToGeonamesIdGenerator(getAllCountriesInFieldNames(), admCode));
  }

  public static DataSet<Row> readAllCountries(ExecutionEnvironment env) {
    final Path allCountriesPath = new Path(BASE_DIR, ALL_COUNTRIES_FILENAME);
    final String[] allCountriesInFn = getAllCountriesInFieldNames();
    final String allCountriesInHeader = String.join(FIELD_DELIM_STR, allCountriesInFn);
    return env//
        .readFile(new TextInputFormat(allCountriesPath), allCountriesPath.toString())//
        .flatMap(new CsvLine2Row(allCountriesInHeader, false, FIELD_DELIM, null))//
        .returns(new RowTypeInfo(FlinkUtils.getDefaultFlinkFieldTypes(allCountriesInFn)));
  }

  private static void writeLocationsFile(final Map<String, String> outFilesMap,
      final BatchTableEnvironment tableEnv, final String[] locationsOutFn,
      final DataSet<LabelledRow> locations) {
    for (Entry<String, String> filterEntry : outFilesMap.entrySet()) {
      final String filterKey = filterEntry.getKey();
      final String locFile = outFilesMap.get(filterEntry.getKey()) + OUT_FILENAME_LOCATIONS;
      final DataSet<Row> outDs = locations.filter(t -> t.f0.equals(filterKey))//
          .map(t -> t.f1).returns(LabelledRow.getRowTypeInfo(locationsOutFn));
      tableEnv.fromDataSet(outDs)//
          .writeToSink(new CsvTableSink(locFile, FIELD_DELIM_STR, 1, WriteMode.OVERWRITE));
    }
  }

}
