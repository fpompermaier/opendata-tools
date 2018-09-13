package it.okkam.opendata.geonames.flink.model;

import it.okkam.opendata.geonames.GeoNamesUtils;

import org.apache.flink.api.java.tuple.Tuple3;

public class AdmCodeTuple extends Tuple3<String, String, String> {

  private static final long serialVersionUID = 1L;

  public AdmCodeTuple() {}

  public AdmCodeTuple(String admCode, String geonamesId, String lastUpdate) {
    this.f0 = admCode;
    this.f1 = GeoNamesUtils.getGeonamesUrl(geonamesId);
    this.f2 = GeoNamesUtils.getStateId(geonamesId, lastUpdate);
  }

  public String getGeonamesUrl() {
    return f1;
  }

  public String getStateId() {
    return f2;
  }

}
