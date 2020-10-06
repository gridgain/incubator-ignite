package org.apache.ignite.internal.configuration.initpojo;

import org.apache.ignite.internal.configuration.setpojo.Builder;

import static org.apache.ignite.internal.configuration.Keys.ENABLED;
import static org.apache.ignite.internal.configuration.Keys.TIMEOUT;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class InitAutoAdjust extends Builder {

    public InitAutoAdjust timeout(long timeout){
        changes.put(TIMEOUT, timeout);

        return this;
    }

    public InitAutoAdjust enabled(boolean enabled){
        changes.put(ENABLED, enabled);

        return this;
    }

    public static InitAutoAdjust initAutoAdjust(){
        return new InitAutoAdjust();
    }
}
