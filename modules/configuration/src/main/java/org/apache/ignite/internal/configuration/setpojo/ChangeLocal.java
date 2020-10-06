package org.apache.ignite.internal.configuration.setpojo;

import static org.apache.ignite.internal.configuration.Keys.BASELINE;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class ChangeLocal extends Builder {
    public ChangeLocal with(ChangeBaseline builder){
        changes.put(BASELINE, builder);

        return this;
    }

    public static ChangeLocal changeLocal(){
        return new ChangeLocal();
    }
}
