package it.okkam.opendata.geonames.jobs;

import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_COUNTRY_CODE;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_FEATURE_CLASS;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_FEATURE_CODE;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_LAST_UPDATE;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_NAME;
import static it.okkam.opendata.geonames.GeoNamesUtils.COL_GEONAMES_ID;
import static it.okkam.opendata.geonames.GeoNamesUtils.FIELD_DELIM_STR;
import static it.okkam.opendata.geonames.GeoNamesUtils.HIERARCHY_COL_CHILD;
import static it.okkam.opendata.geonames.GeoNamesUtils.HIERARCHY_COL_PARENT;
import static it.okkam.opendata.geonames.GeoNamesUtils.OUT_DIR;
import static it.okkam.opendata.geonames.GeoNamesUtils.PREFIX_CHILD;
import static it.okkam.opendata.geonames.GeoNamesUtils.PREFIX_PARENT;
import static it.okkam.opendata.geonames.GeoNamesUtils.getFiltersMap;
import static it.okkam.opendata.geonames.GeoNamesUtils.getHierarchyExtraFields;

import it.okkam.opendata.geonames.GeoNamesUtils;
import it.okkam.opendata.geonames.flink.AllCountriesRowFilter;
import it.okkam.opendata.geonames.flink.FlinkUtils;
import it.okkam.opendata.geonames.flink.HierarchyEnricher;
import it.okkam.opendata.geonames.flink.RowJoiner;

import java.util.Map;
import java.util.Set;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;

public class GeoNamesHierarchyExporter {
  
  private GeoNamesHierarchyExporter() {
    throw new IllegalArgumentException("Runner class");
  }

  private static String[] getAllCountriesFieldsToJoin() {
    return new String[] {//
        COL_GEONAMES_ID, //
        ALL_COUNTRIES_COL_COUNTRY_CODE, //
        ALL_COUNTRIES_COL_NAME, //
        ALL_COUNTRIES_COL_FEATURE_CLASS, //
        ALL_COUNTRIES_COL_FEATURE_CODE, //
        ALL_COUNTRIES_COL_LAST_UPDATE//
    };
  }

  public static void run(String targetCountry, String[] featureFilters,
      Set<String> countryParentIds) throws Exception {
    final ExecutionEnvironment env = FlinkUtils.getExecutionEnv();
    final BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
    final String[] allCountriesProjFn = getAllCountriesFieldsToJoin();
    final Map<String, String[]> filtersMap = getFiltersMap(featureFilters);

    // 1 - read allCountries but keep only interesting rows
    final DataSet<Row> allCountriesRows =
        GeoNamesUtils.readAllCountries(env, allCountriesProjFn).filter(new AllCountriesRowFilter(
            allCountriesProjFn, targetCountry, filtersMap, countryParentIds));

    // 2 - enrich hierarchy parent data
    final String[] hierarchyInFn = GeoNamesUtils.getHierarchyInFieldNames();
    final DataSet<Row> hierarchyRows = GeoNamesUtils.getHierarchyRows(env);
    final String[] parentSuffixedInFn = getSuffixedColumNames(PREFIX_PARENT, allCountriesProjFn);
    final String[] enrichedOutFn1 = GeoNamesUtils.joinArrays(hierarchyInFn, parentSuffixedInFn);
    final int allCountriesIdFieldPos = FlinkUtils.getFieldPos(allCountriesProjFn, COL_GEONAMES_ID);
    final int hierarchyParentIdFpos = FlinkUtils.getFieldPos(hierarchyInFn, HIERARCHY_COL_PARENT);
    final DataSet<Row> parentEnrichedDs = hierarchyRows.join(allCountriesRows)//
        .where(r -> (String) r.getField(hierarchyParentIdFpos))//
        .equalTo(r -> (String) r.getField(allCountriesIdFieldPos))
        .with(new RowJoiner(enrichedOutFn1.length))//
        .returns(FlinkUtils.getDefaultRowTypeInfo(enrichedOutFn1));

    // 3 - enrich hierarchy child data
    final int hierarchyChildIdFpos = FlinkUtils.getFieldPos(enrichedOutFn1, HIERARCHY_COL_CHILD);
    final String[] childSuffixedInFn = getSuffixedColumNames(PREFIX_CHILD, allCountriesProjFn);
    final String[] enrichedOutFn2 = GeoNamesUtils.joinArrays(enrichedOutFn1, childSuffixedInFn);
    final DataSet<Row> hiearchyEnrichedDs = parentEnrichedDs.join(allCountriesRows)//
        .where(r -> (String) r.getField(hierarchyChildIdFpos))//
        .equalTo(r -> (String) r.getField(allCountriesIdFieldPos))
        .with(new RowJoiner(enrichedOutFn2.length))//
        .returns(FlinkUtils.getDefaultRowTypeInfo(enrichedOutFn2));

    // add parent/child url and state-id columns (extra fields)
    final String[] outFn = GeoNamesUtils.joinArrays(enrichedOutFn2, getHierarchyExtraFields());
    final DataSet<Row> hiearchyEnrichedDs2 = hiearchyEnrichedDs//
        .map(new HierarchyEnricher(outFn))//
        .returns(FlinkUtils.getDefaultRowTypeInfo(outFn));

    final String outFile =
        "file:" + java.nio.file.Paths.get(OUT_DIR, targetCountry, "hierarchy.tsv");
    tableEnv.fromDataSet(hiearchyEnrichedDs2)//
        .writeToSink(new CsvTableSink(outFile, FIELD_DELIM_STR, 1, WriteMode.OVERWRITE));

    env.execute("Geonames hierarchy transformer");

  }

  private static String[] getSuffixedColumNames(String suffix, String... columnNames) {
    String[] ret = new String[columnNames.length];
    for (int i = 0; i < columnNames.length; i++) {
      ret[i] = suffix + columnNames[i];
    }
    return ret;
  }
}
