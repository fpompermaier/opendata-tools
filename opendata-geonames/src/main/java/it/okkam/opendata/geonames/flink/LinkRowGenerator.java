package it.okkam.opendata.geonames.flink;

import static it.okkam.opendata.geonames.GeoNamesUtils.ALTNAMES_COL_NAME;

import it.okkam.opendata.geonames.GeoNamesUtils;

import java.net.URL;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class LinkRowGenerator implements MapFunction<Row, Row> {
  private static final long serialVersionUID = 1L;
  private final int nameColumnPos;
  private Row linkRow;

  public LinkRowGenerator(Map<String, Integer> altNamesEnrichedFieldPos, String[] linksOutFn) {
    this.linkRow = new Row(altNamesEnrichedFieldPos.size());
    this.nameColumnPos = altNamesEnrichedFieldPos.get(ALTNAMES_COL_NAME);
    this.linkRow = new Row(linksOutFn.length);
  }

  @Override
  public Row map(Row row) throws Exception {
    final String link = (String) row.getField(nameColumnPos);
    final String host = new URL(link).getHost();
    final String[] hostSplit = host.split(Pattern.quote("."));
    for (int i = 0; i < row.getArity(); i++) {
      linkRow.setField(i, row.getField(i));
    }
    if (!GeoNamesUtils.isIpAddress(host)) {
      linkRow.setField(linkRow.getArity() - 2, GeoNamesUtils.getLinkDomain(hostSplit));
      linkRow.setField(linkRow.getArity() - 1, GeoNamesUtils.getLinkLang(hostSplit));
    }
    return linkRow;
  }


}
