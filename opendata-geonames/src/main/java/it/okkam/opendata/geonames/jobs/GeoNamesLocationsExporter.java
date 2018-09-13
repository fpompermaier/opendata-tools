package it.okkam.opendata.geonames.jobs;

import static it.okkam.opendata.geonames.GeoNamesUtils.ADM5_COL_ADM5;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_ADM1;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_ADM2;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_ADM3;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_ADM4;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_FEATURE_CLASS;
import static it.okkam.opendata.geonames.GeoNamesUtils.FEAT_CODE_ADM1;
import static it.okkam.opendata.geonames.GeoNamesUtils.FEAT_CODE_ADM2;
import static it.okkam.opendata.geonames.GeoNamesUtils.FEAT_CODE_ADM3;
import static it.okkam.opendata.geonames.GeoNamesUtils.FEAT_CODE_ADM4;
import static it.okkam.opendata.geonames.GeoNamesUtils.*;
import static it.okkam.opendata.geonames.GeoNamesUtils.FEAT_CODE_ERROR;
import static it.okkam.opendata.geonames.GeoNamesUtils.FIELD_DELIM_STR;
import static it.okkam.opendata.geonames.GeoNamesUtils.OUT_DIR;
import static it.okkam.opendata.geonames.GeoNamesUtils.OUT_FILENAME_LOCATIONS;
import static it.okkam.opendata.geonames.GeoNamesUtils.SUFFIX_CODE_COL;
import static it.okkam.opendata.geonames.GeoNamesUtils.SUFFIX_GEONAMES_COL;
import static it.okkam.opendata.geonames.GeoNamesUtils.SUFFIX_STATE_ID_COL;
import static it.okkam.opendata.geonames.GeoNamesUtils.getAllCountriesOutFieldNames;
import static it.okkam.opendata.geonames.GeoNamesUtils.getFieldPosMap;
import static it.okkam.opendata.geonames.GeoNamesUtils.getFiltersMap;
import static it.okkam.opendata.geonames.GeoNamesUtils.getOutFileNamesMap;
import static it.okkam.opendata.geonames.GeoNamesUtils.readAdmCodes;
import static it.okkam.opendata.geonames.GeoNamesUtils.readAllCountries;

import it.okkam.opendata.geonames.GeoNamesUtils;
import it.okkam.opendata.geonames.flink.AdmCodeJoiner;
import it.okkam.opendata.geonames.flink.AllCountriesEnricher;
import it.okkam.opendata.geonames.flink.FlinkUtils;
import it.okkam.opendata.geonames.flink.model.AdmCodeTuple;
import it.okkam.opendata.geonames.flink.model.LabelledRow;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;

public class GeoNamesLocationsExporter {

  private GeoNamesLocationsExporter() {
    throw new IllegalArgumentException("Runner class");
  }

  private static boolean printAdmErrors = false;

  public static void run(String targetCountry, String[] featureFilters,
      Set<String> countryParentIds) throws Exception {
    final Map<String, String[]> filtersMap = getFiltersMap(featureFilters);
    final Map<String, String> outFilesMap = getOutFileNamesMap(OUT_DIR, targetCountry, filtersMap);

    final ExecutionEnvironment env = FlinkUtils.getExecutionEnv();
    final BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

    // 1 - read allCountries (with ADM5)
    final DataSet<Row> allCountriesInRows = readAllCountries(env);
    
    // 2 - filter (and enrich) rows by target country and feature filters
    final String[] allCountriesOutFn = getAllCountriesOutFieldNames();
    final Map<String, Integer> allCountriesOutFpos = getFieldPosMap(allCountriesOutFn);
    final TupleTypeInfo<LabelledRow> allCountriesOutRti =
        LabelledRow.getTypeInfo(allCountriesOutFn);
    DataSet<LabelledRow> allCountriesOutRows = allCountriesInRows//
        .flatMap(new AllCountriesEnricher(allCountriesOutFpos, filtersMap, targetCountry,
            countryParentIds))//
        .returns(allCountriesOutRti);

    // 3 - join with ADMs and retrieve thei geoname URLs and state ids 
    allCountriesOutRows = addAdmUrls(allCountriesInRows, allCountriesOutRows, allCountriesOutFpos,
        allCountriesOutRti);

    // 4 - output to different directories 
    writeLocationsFile(outFilesMap, tableEnv, allCountriesOutFn, allCountriesOutRows);

    env.execute("Geonames pivot rows transformer");

  }

  private static DataSet<LabelledRow> addAdmUrls(DataSet<Row> inRows, DataSet<LabelledRow> outRows,
      Map<String, Integer> outFpos, TupleTypeInfo<LabelledRow> outRti) {
    final int adm1CodePos = outFpos.get(ALL_COUNTRIES_COL_ADM1 + SUFFIX_CODE_COL);
    final int adm2CodePos = outFpos.get(ALL_COUNTRIES_COL_ADM2 + SUFFIX_CODE_COL);
    final int adm3CodePos = outFpos.get(ALL_COUNTRIES_COL_ADM3 + SUFFIX_CODE_COL);
    final int adm4CodePos = outFpos.get(ALL_COUNTRIES_COL_ADM4 + SUFFIX_CODE_COL);
    final int adm5CodePos = outFpos.get(ADM5_COL_ADM5 + SUFFIX_CODE_COL);
    final int adm1UrlPos = outFpos.get(ALL_COUNTRIES_COL_ADM1 + SUFFIX_GEONAMES_COL);
    final int adm2UrlPos = outFpos.get(ALL_COUNTRIES_COL_ADM2 + SUFFIX_GEONAMES_COL);
    final int adm3UrlPos = outFpos.get(ALL_COUNTRIES_COL_ADM3 + SUFFIX_GEONAMES_COL);
    final int adm4UrlPos = outFpos.get(ALL_COUNTRIES_COL_ADM4 + SUFFIX_GEONAMES_COL);
    final int adm5UrlPos = outFpos.get(ADM5_COL_ADM5 + SUFFIX_GEONAMES_COL);
    final int adm1StatePos = outFpos.get(ALL_COUNTRIES_COL_ADM1 + SUFFIX_STATE_ID_COL);
    final int adm2StatePos = outFpos.get(ALL_COUNTRIES_COL_ADM2 + SUFFIX_STATE_ID_COL);
    final int adm3StatePos = outFpos.get(ALL_COUNTRIES_COL_ADM3 + SUFFIX_STATE_ID_COL);
    final int adm4StatePos = outFpos.get(ALL_COUNTRIES_COL_ADM4 + SUFFIX_STATE_ID_COL);
    final int adm5StatePos = outFpos.get(ADM5_COL_ADM5 + SUFFIX_STATE_ID_COL);

    final String[] inFns = GeoNamesUtils.getAllCountriesWithAdm5InFieldNames();
    final Map<String, Integer> inFpos = getFieldPosMap(inFns);
    final Integer fClassPos = inFpos.get(ALL_COUNTRIES_COL_FEATURE_CLASS);
    final DataSet<Row> admRows = inRows.filter(r -> FEAT_CLASS_ADM.equals(r.getField(fClassPos)));
    final DataSet<AdmCodeTuple> adm1Codes = readAdmCodes(admRows, FEAT_CODE_ADM1);
    final DataSet<AdmCodeTuple> adm2Codes = readAdmCodes(admRows, FEAT_CODE_ADM2);
    final DataSet<AdmCodeTuple> adm3Codes = readAdmCodes(admRows, FEAT_CODE_ADM3);
    final DataSet<AdmCodeTuple> adm4Codes = readAdmCodes(admRows, FEAT_CODE_ADM4);
    final DataSet<AdmCodeTuple> adm5Codes = readAdmCodes(admRows, FEAT_CODE_ADM5);

    // add all ADM URLs and State IDs
    outRows = joinAdm(outRows, outRti, adm1Codes, adm1CodePos, adm1UrlPos, adm1StatePos);
    outRows = joinAdm(outRows, outRti, adm2Codes, adm2CodePos, adm2UrlPos, adm2StatePos);
    outRows = joinAdm(outRows, outRti, adm3Codes, adm3CodePos, adm3UrlPos, adm3StatePos);
    outRows = joinAdm(outRows, outRti, adm4Codes, adm4CodePos, adm4UrlPos, adm4StatePos);
    outRows = joinAdm(outRows, outRti, adm5Codes, adm5CodePos, adm5UrlPos, adm5StatePos);

    if (printAdmErrors) {
      try {
        adm1Codes.filter(t -> FEAT_CODE_ERROR.equals(t.f0))
            .union(adm2Codes.filter(t -> FEAT_CODE_ERROR.equals(t.f0)))
            .union(adm3Codes.filter(t -> FEAT_CODE_ERROR.equals(t.f0)))
            .union(adm4Codes.filter(t -> FEAT_CODE_ERROR.equals(t.f0)))//
            .print();
      } catch (Exception e) {
        // ignore errors
      }
    }
    return outRows;
  }

  private static UnionOperator<LabelledRow> joinAdm(DataSet<LabelledRow> allCountriesEnriched,
      TupleTypeInfo<LabelledRow> allCountriesEnrichedRti, final DataSet<AdmCodeTuple> admTuples,
      final int admCodePos, final int admUrlPos, final int admStatePos) {
    return allCountriesEnriched.filter(t -> t.f1.getField(admCodePos) == null).union(//
        allCountriesEnriched.filter(t -> t.f1.getField(admCodePos) != null) //
            .leftOuterJoin(admTuples.filter(t -> !FEAT_CODE_ERROR.equals(t.f0)))//
            .where(t -> (String) t.f1.getField(admCodePos)).equalTo(0)//
            .with(new AdmCodeJoiner(admUrlPos, admStatePos))//
            .returns(allCountriesEnrichedRti));
  }

  private static void writeLocationsFile(final Map<String, String> outFilesMap,
      final BatchTableEnvironment tableEnv, final String[] locationsOutFn,
      final DataSet<LabelledRow> locations) {
    for (Entry<String, String> filterEntry : outFilesMap.entrySet()) {
      final String filterKey = filterEntry.getKey();
      final String locFile = outFilesMap.get(filterEntry.getKey()) + OUT_FILENAME_LOCATIONS;
      final DataSet<Row> outDs = locations.filter(t -> t.f0.equals(filterKey))//
          .map(t -> t.f1).returns(FlinkUtils.getDefaultRowTypeInfo(locationsOutFn));
      tableEnv.fromDataSet(outDs)//
          .writeToSink(new CsvTableSink(locFile, FIELD_DELIM_STR, 1, WriteMode.OVERWRITE));
    }
  }

}
