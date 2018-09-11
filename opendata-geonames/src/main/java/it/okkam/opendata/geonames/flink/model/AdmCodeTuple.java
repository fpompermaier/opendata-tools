package it.okkam.opendata.geonames.flink.model;

import org.apache.flink.api.java.tuple.Tuple2;

public class AdmCodeTuple extends Tuple2<String, String> {

  private static final long serialVersionUID = 1L;

  public AdmCodeTuple() {}

  public AdmCodeTuple(String admCode, String geonamesUrl) {
    this.f0 = admCode;
    this.f1 = geonamesUrl;
  }

  public String getGeonamesUrl() {
    return f1;
  }

}
