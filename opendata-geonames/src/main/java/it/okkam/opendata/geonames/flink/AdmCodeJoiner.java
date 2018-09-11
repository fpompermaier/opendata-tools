package it.okkam.opendata.geonames.flink;

import it.okkam.opendata.geonames.flink.model.AdmCodeTuple;
import it.okkam.opendata.geonames.flink.model.LabelledRow;

import org.apache.flink.api.common.functions.JoinFunction;

public class AdmCodeJoiner
    implements JoinFunction<LabelledRow, AdmCodeTuple, LabelledRow> {

  private static final long serialVersionUID = 1L;
  private final int admGeonamesUrlPos;

  public AdmCodeJoiner(int admGeonamesUrlPos) {
    this.admGeonamesUrlPos = admGeonamesUrlPos;
  }

  @Override
  public LabelledRow join(LabelledRow loc, AdmCodeTuple admCode) throws Exception {
    if (admCode == null) {
      return loc;
    }
    loc.getRow().setField(admGeonamesUrlPos, admCode.getGeonamesUrl());
    return loc;
  }

}
