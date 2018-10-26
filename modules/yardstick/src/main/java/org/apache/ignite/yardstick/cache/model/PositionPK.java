package org.apache.ignite.yardstick.cache.model;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class PositionPK {

  @AffinityKeyMapped
  @QuerySqlField(index = true)
  private  String EODPositionID;

  public PositionPK(String EODPositionID) {
    this.EODPositionID = EODPositionID;
  }

  public PositionPK() {
  }

  public String getEODPositionID() {
    return EODPositionID;
  }

  public void setEODPositionID(String EODPositionID) {
    this.EODPositionID = EODPositionID;
  }
}
