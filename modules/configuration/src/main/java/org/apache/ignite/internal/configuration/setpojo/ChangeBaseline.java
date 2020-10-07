package org.apache.ignite.internal.configuration.setpojo;

import static org.apache.ignite.internal.configuration.Keys.AUTO_ADJUST;
import static org.apache.ignite.internal.configuration.Keys.NODE;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class ChangeBaseline extends Builder {
    public ChangeBaseline with(ChangeAutoAdjust builder){
        changes.put(AUTO_ADJUST, builder);

        return this;
    }

    public ChangeBaseline with(NList<ChangeNode> nodes){
        changes.put(NODE, nodes);

        return this;
    }

    public static ChangeBaseline changeBaseline(){
        return new ChangeBaseline();
    }
}
