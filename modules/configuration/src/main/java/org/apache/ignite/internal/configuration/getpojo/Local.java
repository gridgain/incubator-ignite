package org.apache.ignite.internal.configuration.getpojo;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class Local {
    private final Baseline baseline;

    public Local(Baseline baseline) {
        this.baseline = baseline;
    }

    public Baseline baseline() {
        return baseline;
    }

    @Override public String toString() {
        return "Local{" +
            "baseline=" + baseline +
            '}';
    }
}
