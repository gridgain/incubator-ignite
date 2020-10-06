package org.apache.ignite.internal.configuration.setpojo;

import static org.apache.ignite.internal.configuration.Keys.ENABLED;
import static org.apache.ignite.internal.configuration.Keys.TIMEOUT;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class ChangeAutoAdjust extends Builder {

    public ChangeAutoAdjust timeout(long timeout){
        changes.put(TIMEOUT, timeout);

        return this;
    }

    public ChangeAutoAdjust enabled(boolean enabled){
        changes.put(ENABLED, enabled);

        return this;
    }

    public static ChangeAutoAdjust changeAutoAdjust(){
        return new ChangeAutoAdjust();
    }
}
