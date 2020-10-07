package org.apache.ignite.internal.configuration.initpojo;

import org.apache.ignite.internal.configuration.setpojo.Builder;
import org.apache.ignite.internal.configuration.setpojo.NList;

import static org.apache.ignite.internal.configuration.Keys.AUTO_ADJUST;
import static org.apache.ignite.internal.configuration.Keys.NODE;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class InitBaseline extends Builder {
    public InitBaseline with(InitAutoAdjust builder){
        changes.put(AUTO_ADJUST, builder);

        return this;
    }

    public InitBaseline with(NList<InitNode> nodes){
        changes.put(NODE, nodes);

        return this;
    }

    public static InitBaseline initBaseline(){
        return new InitBaseline();
    }
}
