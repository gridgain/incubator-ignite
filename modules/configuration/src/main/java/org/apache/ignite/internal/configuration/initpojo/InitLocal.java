package org.apache.ignite.internal.configuration.initpojo;

import org.apache.ignite.internal.configuration.setpojo.Builder;

import static org.apache.ignite.internal.configuration.Keys.BASELINE;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class InitLocal extends Builder {
    public InitLocal with(InitBaseline builder){
        changes.put(BASELINE, builder);

        return this;
    }

    public static InitLocal initLocal(){
        return new InitLocal();
    }
}
