package it.okkam.opendata.geonames;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.regex.Pattern;

public class GeoNamesDumpUtils {

  private static final String SEPARATOR_PATTERN = Pattern.quote("|");
  private static final String FIELD_DELIM = "\t";

  private static final int ALL_COUNTRIES_COL_GEONAMES_ID = 0;
  private static final int ALL_COUNTRIES_COL_NAME = 1;
  @SuppressWarnings("unused")
  private static final int ALL_COUNTRIES_COL_NAME_ASCII = 2;
  @SuppressWarnings("unused")
  private static final int ALL_COUNTRIES_COL_ALT_NAMES = 3;
  private static final int ALL_COUNTRIES_COL_LAT = 4;
  private static final int ALL_COUNTRIES_COL_LON = 5;
  private static final int ALL_COUNTRIES_COL_FEATURE_CLASS = 6;
  private static final int ALL_COUNTRIES_COL_FEATURE_CODE = 7;
  private static final int ALL_COUNTRIES_COL_COUNTRY_CODE = 8;
  private static final int ALL_COUNTRIES_COL_POPULATION = 14;
  private static final int ALL_COUNTRIES_COL_LAST_UPDATE = 18;

  private static final int[] COLS_TO_PROJECT = new int[] {//
      ALL_COUNTRIES_COL_GEONAMES_ID, //
      ALL_COUNTRIES_COL_NAME, //
      ALL_COUNTRIES_COL_LAT, //
      ALL_COUNTRIES_COL_LON, //
      ALL_COUNTRIES_COL_FEATURE_CLASS, //
      ALL_COUNTRIES_COL_FEATURE_CODE, //
      ALL_COUNTRIES_COL_POPULATION, //
      ALL_COUNTRIES_COL_LAST_UPDATE//
  };

  private static final String[] FEATURES_FILTER = new String[] {//
      "IT|A|*", // Italian Administrative divisions
      "L|CONT", // all continents
      "IT|P|*",// Italian populated places
  };


  private static String inFileName = "/tmp/allCountries.txt";
  private static String outdir = "/tmp/geonames/";

  public static void main(String[] args) throws IOException {
    int index = 0;
    final String outLineformat = getOutLineFormat();
    Map<String, String[]> filtersMap = getFiltersMap();
    Map<String, Path> outFilesMap = getOutFileNamesMap(filtersMap);
    Map<String, BufferedWriter> outWritersMap = getWritersMap(outFilesMap);

    try (Scanner scanner = new Scanner(new File(inFileName));) {
      while (scanner.hasNext()) {
        final String line = scanner.nextLine();
        final String[] tokens = line.split(FIELD_DELIM);
        for (Entry<String, String[]> filterEntry : filtersMap.entrySet()) {
          if (checkFilterConf(tokens, filterEntry.getValue())) {
            final BufferedWriter writer = outWritersMap.get(filterEntry.getKey());
            writer.write(String.format(outLineformat, getOutTokens(tokens)));
          }
        }
        // do something with line
        index++;
        if (index % 100000 == 0) {
          System.out.println("Processed " + index + " lines...");// NOSONAR
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      for (BufferedWriter writer : outWritersMap.values()) {
        writer.close();
      }
    }

    System.out.println("Done.");
  }

  private static Map<String, BufferedWriter> getWritersMap(Map<String, Path> outFilesMap)
      throws IOException {
    final Map<String, BufferedWriter> ret = new HashMap<>();
    final Charset charset = Charset.forName("UTF-8");
    for (Entry<String, Path> outFileEntry : outFilesMap.entrySet()) {
      final Path outFile = outFileEntry.getValue().resolve("data.txt");
      ret.put(outFileEntry.getKey(), Files.newBufferedWriter(outFile, charset));
    }
    return ret;
  }

  private static Map<String, Path> getOutFileNamesMap(Map<String, String[]> filtersMap) {
    final Map<String, Path> ret = new HashMap<>();
    for (String filterStr : filtersMap.keySet()) {
      final String[] tokens = filterStr.split(SEPARATOR_PATTERN);
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = tokens[i].equals("*") ? "ALL" : tokens[i];
      }
      final Path path = Paths.get(outdir, tokens);
      if (!path.toFile().exists()) {
        path.toFile().mkdirs();
      }
      ret.put(filterStr, path);
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

  // return true iff feature class and code and country match the passed filter
  private static boolean checkFilterConf(String[] lineTokens, String[] filter) {
    final String featureClass = lineTokens[ALL_COUNTRIES_COL_FEATURE_CLASS].trim();
    final String featureCode = lineTokens[ALL_COUNTRIES_COL_FEATURE_CODE].trim();
    final String countryCode = lineTokens[ALL_COUNTRIES_COL_COUNTRY_CODE].trim();
    if (filter.length < 3) {
      final boolean validFeatureClass = filter[0].equals("*") || filter[0].equals(featureClass);
      final boolean validFeatureCode = filter[1].equals("*") || filter[1].equals(featureCode);
      return validFeatureClass && validFeatureCode;
    }
    final boolean validCountryCode = filter[0].equals("*") || filter[0].equals(countryCode);
    final boolean validFeatureClass = filter[1].equals("*") || filter[1].equals(featureClass);
    final boolean validFeatureCode = filter[2].equals("*") || filter[2].equals(featureCode);
    return validFeatureClass && validFeatureCode && validCountryCode;
  }

  private static Object[] getOutTokens(final String[] tokens) {
    final Object[] outValues = new String[COLS_TO_PROJECT.length + 1];
    outValues[0] = getGeonamesUrl(tokens);
    for (int i = 0; i < COLS_TO_PROJECT.length; i++) {
      outValues[i + 1] = tokens[COLS_TO_PROJECT[i]];
    }
    return outValues;
  }

  private static String getGeonamesUrl(final String[] tokens) {
    return "http://www.geonames.org/" + tokens[ALL_COUNTRIES_COL_GEONAMES_ID];
  }

  private static String getOutLineFormat() {
    final StringBuilder outLineFormat = new StringBuilder();
    // add Geonames URL as first column
    for (int i = 0; i < COLS_TO_PROJECT.length + 1; i++) {
      outLineFormat.append("%s");
      if (i < COLS_TO_PROJECT.length) {
        outLineFormat.append(FIELD_DELIM);
      } else {
        outLineFormat.append("\n");
      }
    }
    return outLineFormat.toString();
  }
}
