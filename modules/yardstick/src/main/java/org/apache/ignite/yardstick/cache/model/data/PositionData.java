package org.apache.ignite.yardstick.cache.model.data;

import java.util.List;
import org.apache.ignite.yardstick.cache.model.Position;
import org.apache.ignite.yardstick.cache.model.PositionPK;
import org.apache.ignite.yardstick.cache.model.Sensitivity;
import org.apache.ignite.yardstick.cache.model.SensitivityPK;

public class PositionData {

    private KVPair<PositionPK, Position> position;

    private List<KVPair<SensitivityPK, Sensitivity>> s;

    public PositionData() {
    }

    public PositionData(
        KVPair<PositionPK, Position> position,
        List<KVPair<SensitivityPK, Sensitivity>> s) {
        this.position = position;
        this.s = s;
    }

    public KVPair<PositionPK, Position> getPosition() {
        return position;
    }

    public void setPosition(
        KVPair<PositionPK, Position> position) {
        this.position = position;
    }

    public List<KVPair<SensitivityPK, Sensitivity>> getS() {
        return s;
    }

    public void setS(
        List<KVPair<SensitivityPK, Sensitivity>> s) {
        this.s = s;
    }
}
