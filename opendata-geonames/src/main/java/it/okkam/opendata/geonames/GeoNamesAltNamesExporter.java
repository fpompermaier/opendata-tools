package it.okkam.opendata.geonames;

import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_OUT_EXTRA_COLS;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALTNAMES_COL_LANG;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALT_NAMES_FILENAME;
import static it.okkam.opendata.geonames.GeoNamesUtils.BASE_DIR;
import static it.okkam.opendata.geonames.GeoNamesUtils.COL_GEONAMES_ID;
import static it.okkam.opendata.geonames.GeoNamesUtils.FIELD_DELIM;
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
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_RU;
import static it.okkam.opendata.geonames.GeoNamesUtils.LANG_WIKIDATA;
import static it.okkam.opendata.geonames.GeoNamesUtils.LINKS_COL_DOMAIN;
import static it.okkam.opendata.geonames.GeoNamesUtils.LINKS_COL_LANG;
import static it.okkam.opendata.geonames.GeoNamesUtils.OUT_DIR;
import static it.okkam.opendata.geonames.GeoNamesUtils.OUT_FILENAME_ALTNAMES;
import static it.okkam.opendata.geonames.GeoNamesUtils.OUT_FILENAME_LINKS;
import static it.okkam.opendata.geonames.GeoNamesUtils.getAltNamesEnrichedFieldNames;
import static it.okkam.opendata.geonames.GeoNamesUtils.getFieldPosMap;
import static it.okkam.opendata.geonames.GeoNamesUtils.getFiltersMap;
import static it.okkam.opendata.geonames.GeoNamesUtils.getOutFileNamesMap;
import static it.okkam.opendata.geonames.GeoNamesUtils.joinArrays;

import it.okkam.flink.csv.CsvLine2Row;
import it.okkam.opendata.geonames.flink.AllCountriesAltNamesJoiner;
import it.okkam.opendata.geonames.flink.AllCountriesGenerator;
import it.okkam.opendata.geonames.flink.AltNamesGenerator;
import it.okkam.opendata.geonames.flink.FlinkUtils;
import it.okkam.opendata.geonames.flink.LinkRowGenerator;
import it.okkam.opendata.geonames.flink.model.LabelledRow;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;

public class GeoNamesAltNamesExporter {


  private static final String[] FEATURES_FILTER = new String[] {//
      // "L|CONT", // all continents
      "AD|A|*", // Italian administrative divisions
      // "IT|P|*",// Italian populated places
      // "US|A|*", // American administrative divisions
      // "US|P|*", // American populated places
  };

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

  public static void main(String[] args) throws Exception {
    final Map<String, String[]> filtersMap = getFiltersMap(FEATURES_FILTER);
    final Map<String, String> outFilesMap = getOutFileNamesMap(OUT_DIR, filtersMap);

    final ExecutionEnvironment env = FlinkUtils.getExecutionEnv();
    final BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);


    // main locations file
    final DataSet<Row> allCountriesRows = GeoNamesLocationsExporter.readAllCountries(env);
    final String[] locationsOutFn = GeoNamesUtils.getLocationsOutFieldNames();
    final DataSet<LabelledRow> locations = allCountriesRows
        .flatMap(new AllCountriesGenerator(ALL_COUNTRIES_OUT_EXTRA_COLS, filtersMap))//
        .returns(LabelledRow.getTypeInfo(locationsOutFn));

    // alternatives transformation -> links + alternative names
    final String[] altNamesInFn = GeoNamesUtils.getAltNamesInFieldNames();
    final DataSet<Row> altNames = getAltNamesRows(env, altNamesInFn);
    final List<List<String>> altNamesEnrichedFn = getAltNamesEnrichedFieldNames();
    final String[] leftFields = altNamesEnrichedFn.get(0).toArray(new String[0]);
    final String[] rightFields = altNamesEnrichedFn.get(1).toArray(new String[0]);
    final String[] altNamesEnrichedOutFn = GeoNamesUtils.joinArrays(leftFields, rightFields);
    final Map<String, Integer> altNamesEnrichedFieldPos = //
        GeoNamesUtils.getFieldPosMap(altNamesEnrichedOutFn);
    final int locationsGeonamesIdFieldPos = getGeonameIdFieldPos(locationsOutFn);
    final int altNamesGeonamesIdFieldPos = getGeonameIdFieldPos(altNamesInFn);
    final DataSet<LabelledRow> altNamesEnriched = locations.join(altNames)//
        .where(t -> (String) t.getRow().getField(locationsGeonamesIdFieldPos))//
        .equalTo(r -> (String) r.getField(altNamesGeonamesIdFieldPos))//
        .with(new AllCountriesAltNamesJoiner(leftFields, rightFields, altNamesEnrichedFieldPos,
            getFieldPosMap(locationsOutFn), getFieldPosMap(altNamesInFn)))//
        .returns(LabelledRow.getTypeInfo(altNamesEnrichedOutFn));
    writeLangAndLinkFiles(outFilesMap, tableEnv, altNamesEnrichedOutFn, altNamesEnrichedFieldPos,
        altNamesEnriched);

    env.execute("Geonames dump transformer");

  }

  private static int getGeonameIdFieldPos(String[] fieldNames) {
    for (int i = 0; i < fieldNames.length; i++) {
      if (fieldNames[i].equals(COL_GEONAMES_ID)) {
        return i;
      }
    }
    return -1;
  }

  private static void writeLangAndLinkFiles(final Map<String, String> outFilesMap,
      final BatchTableEnvironment tableEnv, final String[] altNamesEnrichedOutFn,
      final Map<String, Integer> altNamesEnrichedFieldPos,
      final DataSet<LabelledRow> altNamesEnriched) {
    final Integer langTypePos = altNamesEnrichedFieldPos.get(ALTNAMES_COL_LANG);
    for (Entry<String, String> filterEntry : outFilesMap.entrySet()) {
      final String filterKey = filterEntry.getKey();
      final DataSet<Row> outDs = altNamesEnriched//
          .filter(t -> t.getLabel().equals(filterKey))//
          .map(LabelledRow::getRow)//
          .returns(LabelledRow.getRowTypeInfo(altNamesEnrichedOutFn));
      final DataSet<Row> altNamesDs = outDs.filter(row -> !isLink(row, langTypePos));
      final DataSet<Row> linkDs = outDs.filter(row -> isLink(row, langTypePos));

      // alternative names
      final String altNamesFile = outFilesMap.get(filterEntry.getKey()) + OUT_FILENAME_ALTNAMES;
      tableEnv.fromDataSet(altNamesDs)//
          .writeToSink(new CsvTableSink(altNamesFile, FIELD_DELIM_STR, 1, WriteMode.OVERWRITE));


      // links
      final String linkFile = outFilesMap.get(filterEntry.getKey()) + OUT_FILENAME_LINKS;
      final String[] extraLinkFields = new String[] {LINKS_COL_DOMAIN, LINKS_COL_LANG};
      final String[] linksOutFn = joinArrays(altNamesEnrichedOutFn, extraLinkFields);
      final DataSet<Row> enrichedLinkDs =
          linkDs.map(new LinkRowGenerator(altNamesEnrichedFieldPos, linksOutFn))
              .returns(LabelledRow.getRowTypeInfo(linksOutFn));
      tableEnv.fromDataSet(enrichedLinkDs)//
          .writeToSink(new CsvTableSink(linkFile, FIELD_DELIM_STR, 1, WriteMode.OVERWRITE));
    }
  }

  private static boolean isLink(Row row, Integer langTypePos) {
    return row.getField(langTypePos) != null && LANG_LINK.equals(row.getField(langTypePos));
  }

  private static DataSet<Row> getAltNamesRows(final ExecutionEnvironment env,
      final String[] altNamesInFn) {
    final Path altNamesPath = new Path(BASE_DIR, ALT_NAMES_FILENAME);
    final RowTypeInfo rowType = new RowTypeInfo(FlinkUtils.getDefaultFlinkFieldTypes(altNamesInFn));
    final String altNamesInHeader = String.join(FIELD_DELIM_STR, altNamesInFn);

    return env.readFile(new TextInputFormat(altNamesPath), altNamesPath.toString())
        .flatMap(new CsvLine2Row(altNamesInHeader, false, FIELD_DELIM, null))//
        .returns(rowType)//
        .flatMap(new AltNamesGenerator(ALTNAMES_LANG_FILTER))//
        .returns(rowType);
  }


}
