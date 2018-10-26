package org.apache.ignite.yardstick.cache.model;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class SensitivityPK {
    @AffinityKeyMapped
    @QuerySqlField(orderedGroups={@QuerySqlField.Group(
            name = "s_eodpositionid_sid_idx", order = 0)})
    private String eodPositionId;

    @QuerySqlField(orderedGroups={@QuerySqlField.Group(
            name = "s_eodpositionid_sid_idx", order = 1)})
    private Long sensitivityMeasureId;

    public SensitivityPK(String eodPositionId, Long sensitivityMeasureId) {
        this.eodPositionId = eodPositionId;
        this.sensitivityMeasureId = sensitivityMeasureId;
    }

    public SensitivityPK() {}

    public Long getSensitivityMeasureId() {
        return sensitivityMeasureId;
    }

    public void setSensitivityMeasureId(Long sensitivityMeasureId) {
        this.sensitivityMeasureId = sensitivityMeasureId;
    }

    public String getEodPositionId() {
        return eodPositionId;
    }

    public void setEodPositionId(String eodPositionId) {
        this.eodPositionId = eodPositionId;
    }
}
