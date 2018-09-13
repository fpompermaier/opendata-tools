package it.okkam.opendata.geonames.jobs;

import static it.okkam.opendata.geonames.GeoNamesUtils.ALTNAMES_COL_LANG;
import static it.okkam.opendata.geonames.GeoNamesUtils.COL_GEONAMES_ID;
import static it.okkam.opendata.geonames.GeoNamesUtils.FIELD_DELIM_STR;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_ABBR;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_DE;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_DEFAULT;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_EN;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_ES;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_FR;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_IT;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_LINK;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_POSTCODE;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_PT;
import static it.okkam.opendata.geonames.GeoNamesUtils.*;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_WIKIDATA;
import static it.okkam.opendata.geonames.GeoNamesUtils.LINKS_COL_DOMAIN;
import static it.okkam.opendata.geonames.GeoNamesUtils.LINKS_COL_LANG;
import static it.okkam.opendata.geonames.GeoNamesUtils.OUT_DIR;
import static it.okkam.opendata.geonames.GeoNamesUtils.OUT_FILENAME_ALTNAMES;
import static it.okkam.opendata.geonames.GeoNamesUtils.OUT_FILENAME_LINKS;
import static it.okkam.opendata.geonames.GeoNamesUtils.getAllCountriesOutFieldNames;
import static it.okkam.opendata.geonames.GeoNamesUtils.getFieldPosMap;
import static it.okkam.opendata.geonames.GeoNamesUtils.getFiltersMap;
import static it.okkam.opendata.geonames.GeoNamesUtils.getOutFileNamesMap;
import static it.okkam.opendata.geonames.GeoNamesUtils.joinArrays;

import it.okkam.opendata.geonames.GeoNamesUtils;
import it.okkam.opendata.geonames.flink.AllCountriesEnricher;
import it.okkam.opendata.geonames.flink.FlinkUtils;
import it.okkam.opendata.geonames.flink.LabelledRowJoiner;
import it.okkam.opendata.geonames.flink.LinkRowGenerator;
import it.okkam.opendata.geonames.flink.model.LabelledRow;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;

public class GeoNamesAltNamesExporter {

  private GeoNamesAltNamesExporter() {
    throw new IllegalArgumentException("Runner class");
  }

  private static final String[] ALTNAMES_LANG_FILTER = new String[] {//
      LANG_DEFAULT, //
      LANG_ABBR, //
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

  public static void run(String targetCountry, String[] featureFilters, Set<String> countryParents)
      throws Exception {
    final Map<String, String[]> filtersMap = getFiltersMap(featureFilters);
    final Map<String, String> outFilesMap = getOutFileNamesMap(OUT_DIR, targetCountry, filtersMap);

    final ExecutionEnvironment env = FlinkUtils.getExecutionEnv();
    final BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);


    final DataSet<Row> allCountriesRows = GeoNamesUtils.readAllCountries(env);
    final String[] allCountriesOutFn = getAllCountriesOutFieldNames();
    final Map<String, Integer> allCountriesFpos = getFieldPosMap(allCountriesOutFn);
    DataSet<LabelledRow> allCountriesLabelled = allCountriesRows
        .flatMap(
            new AllCountriesEnricher(allCountriesFpos, filtersMap, targetCountry, countryParents))
        .returns(LabelledRow.getTypeInfo(allCountriesOutFn));

    // alternatives transformation -> links + alternative names
    final String[] altNamesInFn = GeoNamesUtils.getAltNamesInFieldNames();
    final Map<String, Integer> altNamesInFpos = getFieldPosMap(altNamesInFn);
    final DataSet<Row> altNames = GeoNamesUtils.getAltNamesRows(env, ALTNAMES_LANG_FILTER);
    final List<List<String>> altNamesEnrichedFn = getAltNamesEnrichedFieldNames();
    final String[] allCountriesFn = altNamesEnrichedFn.get(0).toArray(new String[0]);
    final String[] altNamesFn = altNamesEnrichedFn.get(1).toArray(new String[0]);
    final String[] enrichedOutFn = GeoNamesUtils.joinArrays(allCountriesFn, altNamesFn);
    final Map<String, Integer> enrichedFieldPos = GeoNamesUtils.getFieldPosMap(enrichedOutFn);
    final int allCountriesGeonamesIdFieldPos = allCountriesFpos.get(COL_GEONAMES_ID);
    final int altNamesGeonamesIdFieldPos = altNamesInFpos.get(COL_GEONAMES_ID);
    final DataSet<LabelledRow> enrichedDs = allCountriesLabelled.join(altNames)//
        .where(t -> (String) t.getRow().getField(allCountriesGeonamesIdFieldPos))//
        .equalTo(r -> (String) r.getField(altNamesGeonamesIdFieldPos))//
        .with(new LabelledRowJoiner(allCountriesFn, altNamesFn, enrichedFieldPos, allCountriesFpos,
            altNamesInFpos))//
        .returns(LabelledRow.getTypeInfo(enrichedOutFn));
    writeAsCsv(outFilesMap, tableEnv, enrichedOutFn, enrichedFieldPos, enrichedDs);

    env.execute("Geonames altNamesV2 transformer");

  }

  private static void writeAsCsv(final Map<String, String> outFilesMap,
      final BatchTableEnvironment tableEnv, final String[] altNamesEnrichedOutFn,
      final Map<String, Integer> altNamesEnrichedFieldPos, final DataSet<LabelledRow> enrichedDs) {
    final Integer langTypePos = altNamesEnrichedFieldPos.get(ALTNAMES_COL_LANG);
    for (Entry<String, String> filterEntry : outFilesMap.entrySet()) {
      final String filterKey = filterEntry.getKey();
      final DataSet<Row> outDs = enrichedDs//
          .filter(t -> t.getLabel().equals(filterKey))//
          .map(LabelledRow::getRow)//
          .returns(FlinkUtils.getDefaultRowTypeInfo(altNamesEnrichedOutFn));
      final DataSet<Row> altNamesDs = outDs.filter(row -> !isLink(row, langTypePos));
      final DataSet<Row> linkDs = outDs.filter(row -> isLink(row, langTypePos));

      // write alternative names file
      final String altNamesFile = outFilesMap.get(filterEntry.getKey()) + OUT_FILENAME_ALTNAMES;
      tableEnv.fromDataSet(altNamesDs)//
          .writeToSink(new CsvTableSink(altNamesFile, FIELD_DELIM_STR, 1, WriteMode.OVERWRITE));

      // write links file
      final String linkFile = outFilesMap.get(filterEntry.getKey()) + OUT_FILENAME_LINKS;
      final String[] extraLinkFields = new String[] {LINKS_COL_DOMAIN, LINKS_COL_LANG};
      final String[] linksOutFn = joinArrays(altNamesEnrichedOutFn, extraLinkFields);
      final DataSet<Row> enrichedLinkDs =
          linkDs.map(new LinkRowGenerator(altNamesEnrichedFieldPos, linksOutFn))
              .returns(FlinkUtils.getDefaultRowTypeInfo(linksOutFn));
      tableEnv.fromDataSet(enrichedLinkDs)//
          .writeToSink(new CsvTableSink(linkFile, FIELD_DELIM_STR, 1, WriteMode.OVERWRITE));
    }
  }

  private static boolean isLink(Row row, Integer langTypePos) {
    return row.getField(langTypePos) != null && LANG_LINK.equals(row.getField(langTypePos));
  }


}
