package it.okkam.opendata.geonames;

import it.okkam.opendata.geonames.flink.FlinkUtils;
import it.okkam.opendata.geonames.jobs.GeoNamesAltNamesExporter;
import it.okkam.opendata.geonames.jobs.GeoNamesHierarchyExporter;
import it.okkam.opendata.geonames.jobs.GeoNamesLocationsExporter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.types.Row;

public class GeonamesExportByCountry {

  private static String countryFilter = "IT";
  private static String countryId = "3175395";

  private static String[] featureFilters = new String[] {//
      "A=*", //
      // "P=*", //
      // "T=ISL,ISLS,ISLT,ISLX,ISLET,CAPE,PEN,PENX,VAL,VALX,UPLD", //
      "L=AREA,CONT,CST,LAND,LCTY,OAS,RGN,RGNE,RGNH,RGNL"//
  };

  private static boolean exportLocations = true;
  private static boolean exportHierarchy = false;
  private static boolean exportAltNames = false;

  public static void main(String[] args) throws Exception {
    final Set<String> countryParentIds = getCountryParentIds();
    if (exportLocations) {
      GeoNamesLocationsExporter.run(countryFilter, featureFilters, countryParentIds);
    }
    if (exportHierarchy) {
      GeoNamesHierarchyExporter.run(countryFilter, featureFilters, countryParentIds);
    }
    if (exportAltNames) {
      GeoNamesAltNamesExporter.run(countryFilter, featureFilters, countryParentIds);
    }
  }

  private static Set<String> getCountryParentIds() throws Exception {
    final ExecutionEnvironment env = FlinkUtils.getExecutionEnv();
    final DataSet<Row> hierarchyRows = GeoNamesUtils.getHierarchyRows(env);
    final Map<String, List<String>> childerhierarchy = new HashMap<>();
    for (Row row : hierarchyRows.collect()) {
      final String parentId = (String) row.getField(0);
      final String childId = (String) row.getField(1);
      childerhierarchy.computeIfAbsent(childId, (x -> new ArrayList<>())).add(parentId);
    }
    return computeCountryClosure(childerhierarchy, new HashSet<>(), countryId);
  }

  private static Set<String> computeCountryClosure(final Map<String, List<String>> childerhierarchy,
      final Set<String> countryClosure, String parentId) {
    if (parentId != null) {
      countryClosure.add(parentId);
      for (String tmpParentId : childerhierarchy.getOrDefault(parentId, new ArrayList<>(0))) {
        computeCountryClosure(childerhierarchy, countryClosure, tmpParentId);
      }
    }
    return countryClosure;
  }
}
