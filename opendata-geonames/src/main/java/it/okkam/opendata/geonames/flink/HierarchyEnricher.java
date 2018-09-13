package it.okkam.opendata.geonames.flink;

import static it.okkam.opendata.geonames.GeoNamesUtils.ALL_COUNTRIES_COL_LAST_UPDATE;
import static it.okkam.opendata.geonames.GeoNamesUtils.COL_GEONAMES_ID;
import static it.okkam.opendata.geonames.GeoNamesUtils.COL_STATE_ID;
import static it.okkam.opendata.geonames.GeoNamesUtils.PREFIX_CHILD;
import static it.okkam.opendata.geonames.GeoNamesUtils.PREFIX_PARENT;
import static it.okkam.opendata.geonames.GeoNamesUtils.getFieldPosMap;

import it.okkam.opendata.geonames.GeoNamesUtils;

import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class HierarchyEnricher implements MapFunction<Row, Row> {
  private static final long serialVersionUID = 1L;
  private final Row reuse;
  private final int parentGeonamesUrlPos;
  private final int parentStateIdPos;
  private final int parentLastUpdatePos;
  private final int childGeonamesUrlPos;
  private final int childStateIdPos;
  private final int childLastUpdatePos;

  public HierarchyEnricher(String[] outFn) {
    final Map<String, Integer> outPos = getFieldPosMap(outFn);
    this.reuse = new Row(outPos.size());
    this.parentGeonamesUrlPos = outPos.get(PREFIX_PARENT + COL_GEONAMES_ID);
    this.parentStateIdPos = outPos.get(PREFIX_PARENT + COL_STATE_ID);
    this.parentLastUpdatePos = outPos.get(PREFIX_PARENT + ALL_COUNTRIES_COL_LAST_UPDATE);
    this.childGeonamesUrlPos = outPos.get(PREFIX_CHILD + COL_GEONAMES_ID);
    this.childStateIdPos = outPos.get(PREFIX_CHILD + COL_STATE_ID);
    this.childLastUpdatePos = outPos.get(PREFIX_CHILD + ALL_COUNTRIES_COL_LAST_UPDATE);
  }

  @Override
  public Row map(Row row) throws Exception {
    for (int i = 0; i < row.getArity(); i++) {
      reuse.setField(i, row.getField(i));
    }
    final String parentGeonamesId = (String) reuse.getField(parentGeonamesUrlPos);
    final String parentLastUpdate = (String) reuse.getField(parentLastUpdatePos);
    final String childGeonamesId = (String) reuse.getField(childGeonamesUrlPos);
    final String childLastUpdate = (String) reuse.getField(childLastUpdatePos);
    reuse.setField(parentGeonamesUrlPos, GeoNamesUtils.getGeonamesUrl(parentGeonamesId));
    reuse.setField(parentStateIdPos, GeoNamesUtils.getStateId(parentGeonamesId, parentLastUpdate));
    reuse.setField(childGeonamesUrlPos, GeoNamesUtils.getGeonamesUrl(childGeonamesId));
    reuse.setField(childStateIdPos, GeoNamesUtils.getStateId(childGeonamesId, childLastUpdate));
    return reuse;
  }
}
