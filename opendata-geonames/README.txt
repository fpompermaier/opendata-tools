This project aims to rework the available geonames dump available at http://download.geonames.org/export/dump/.

This work considers only a subset of available dataset:

- allCountries.zip: http://download.geonames.org/export/dump/allCountries.zip
- alternateNamesV2.zip: http://download.geonames.org/export/dump/alternateNamesV2.zip
- hierarchy.zip: http://download.geonames.org/export/dump/hierarchy.zip

The listed dataset are described in details at http://download.geonames.org/export/dump/readme.txt.

# Headers

From that file we've generated the following headers:


###### allCountries
> 
GEONAME_ID,NAME,NAME_ASCII,ALT_NAMES,LAT,LON,FEAT_CLASS,FEAT_CODE,COUNTRY_CODE,CC2,ADM1,ADM2,ADM3,ADM4,POPULATION,ELEVATION,DEM,TIMEZONE,LASTUPDATE

###### alternateNamesV2 
> 
ALTNAME_ID,GEONAME_ID,LANG,VALUE,PREFERRED,SHORT,COLLOQUIAL,HISTORICAL,FROM,TO

###### hierarchy
> 
PARENT_ID,CHILD_ID,REL_TYPE

# Produced files

The main goal of this Flink job is to filter all geonames data by Country: in the main class (GeonamesExportByCountry.java) we can also
configure which feature classes and codes to consider.

Those 2 filters right now are hard-coded in that class. 
The ***countryFilter*** variable defines the target Country we want to use as pivot for the data slicing.
The ***featureFilters*** variable defines which feature class and codes to consider with the following syntax:

```
<Feature-class> = <comma-separated-list-of-feature-codes>
```

For example:

```java
 private static String countryFilter = "IT";

 private static String[] featureFilters = new String[] {//
      "A=*", //
      "P=*", //
      "T=ISL,ISLS,ISLT,ISLX,ISLET,CAPE,PEN,PENX,VAL,VALX,UPLD", //
      "L=AREA,CONT,CST,LAND,LCTY,OAS,RGN,RGNE,RGNH,RGNL"//
  };
``` 


This process produces several files:

###### hierarchy.tsv
> 
GEONAMES_ID,NAME,ASCIINAME,ALT_NAMES,LAT,LON,FEAT_CLASS,FEAT_CODE,COUNTRY_CODE,CC2,ADM1,ADM2,ADM3,ADM4,POPULATION,ELEVATION,DEM,TIMEZONE,LASTUPDATE,ADM5,GEONAMES_URL,STATE_ID,ADM1_CODE,ADM1_GEONAMES_URL,ADM1_STATE_ID,ADM2_CODE,ADM2_GEONAMES_URL,ADM2_STATE_ID,ADM3_CODE,ADM3_GEONAMES_URL,ADM3_STATE_ID,ADM4_CODE,ADM4_GEONAMES_URL,ADM4_STATE_ID,ADM5_CODE,ADM5_GEONAMES_URL,ADM5_STATE_ID

###### hierarchy.tsv
>
GEONAMES_URL,GEONAMES_ID,NAME,FEATURE_CLASS,FEATURE_CODE,LANG,VALUE,PREFERRED,SHORT,COLLOQUIAL,HISTORICAL

###### hierarchy.tsv
>
PARENT_ID,CHILD_ID,REL_TYPE,PARENT_GEONAMES_ID,PARENT_COUNTRY_CODE,PARENT_NAME,PARENT_FEAT_CLASS,PARENT_FEAT_CODE,PARENT_LASTUPDATE,CHILD_GEONAMES_ID,CHILD_COUNTRY_CODE,CHILD_NAME,CHILD_FEAT_CLASS,CHILD_FEAT_CODE,CHILD_LASTUPDATE,PARENT__GEONAMES_URL,PARENT__STATE_ID,CHILD__GEONAMES_URL,CHILD__STATE_ID


   
Rules to generate relations:
 
PARENT_FEAT_CLASS == "A" AND CHILD_FEAT_CLASS IN ("A","P") -> hasParentAdmState
PARENT_FEAT_CLASS IN("L","T) AND CHILD_FEAT_CLASS IN ("A","L","P","T") -> hasParentLocationState
PARENT_FEAT_CLASS == "P" AND (CHILD_FEAT_CLASS == "P" ||  (CHILD_FEAT_CLASS == "L" AND CHILD_FEAT_CODE IN ("LCTY"))) -> hasParentLocationState
