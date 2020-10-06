package org.apache.ignite.internal.configuration.getpojo;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class AutoAdjust {
    private final long timeout;
    private final boolean enabled;

    public AutoAdjust(long timeout, boolean enabled) {
        this.timeout = timeout;
        this.enabled = enabled;
    }

    public long timeout() {
        return timeout;
    }

    public boolean enabled() {
        return enabled;
    }

    @Override public String toString() {
        return "AutoAdjust{" +
            "timeout=" + timeout +
            ", enabled=" + enabled +
            '}';
    }
}
