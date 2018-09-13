package it.okkam.opendata.geonames.flink;

import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_COUNTRY_CODE;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_FEATURE_CLASS;
import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_FEATURE_CODE;
import static it.okkam.opendata.geonames.GeoNamesUtils.*;

import it.okkam.opendata.geonames.GeoNamesUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.calcite.shaded.com.google.common.collect.Sets;
import org.apache.flink.types.Row;


public class AllCountriesRowFilter implements FilterFunction<Row> {

  private static final long serialVersionUID = 1L;
  private final int geonamesIdPos;
  private final int featureClassPos;
  private final int featureCodePos;
  private final int countryCodePos;
  private final String targetCountry;
  private final Set<String> targetFeatureClasses;
  private final Map<String, Set<String>> targetFeatureCodes;
  private final Set<String> countryParentIds;

  public AllCountriesRowFilter(String[] fieldNames, String targetCountry,
      Map<String, String[]> filtersMap, Set<String> countryParentIds) {
    final Map<String, Integer> fieldPos = GeoNamesUtils.getFieldPosMap(fieldNames);
    this.geonamesIdPos = fieldPos.get(COL_GEONAMES_ID);
    this.featureClassPos = fieldPos.get(ALL_COUNTRIES_COL_FEATURE_CLASS);
    this.featureCodePos = fieldPos.get(ALL_COUNTRIES_COL_FEATURE_CODE);
    this.countryCodePos = fieldPos.get(ALL_COUNTRIES_COL_COUNTRY_CODE);
    this.targetCountry = targetCountry;
    this.countryParentIds = countryParentIds;
    this.targetFeatureClasses = new HashSet<>();
    this.targetFeatureCodes = new HashMap<>();
    for (Entry<String, String[]> filterEntry : filtersMap.entrySet()) {
      final String featClass = filterEntry.getKey().split(Pattern.quote("="))[0];
      if (!featClass.equals(PARENT_LOCATIONS_FILTER)) {
        targetFeatureClasses.add(featClass);
        final String[] featClasses = filterEntry.getValue();
        if (featClasses.length == 1 && featClasses[0].equals("*")) {
          targetFeatureCodes.put(featClass, null);
        } else {
          targetFeatureCodes.put(featClass, Sets.newHashSet(featClasses));
        }
      }
    }
  }

  @Override
  public boolean filter(Row row) throws Exception {
    // parent ids should pass
    if (countryParentIds.contains(row.getField(geonamesIdPos))) {
      return true;
    }
    // if the row does not belong to the target country do not pass
    if (!targetCountry.equals(row.getField(countryCodePos))) {
      return false;
    }
    // feature class should match filters
    if (!targetFeatureClasses.contains(row.getField(featureClassPos))) {
      return false;
    }
    final Set<String> validFeatureCodes = targetFeatureCodes.get(row.getField(featureClassPos));
    // validFeatureCodes is null when the feature code filter is *
    return validFeatureCodes == null || validFeatureCodes.contains(row.getField(featureCodePos));
  }

}
