package org.apache.ignite.internal.configuration.getpojo;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class Baseline {
    private final AutoAdjust autoAdjust;

    public Baseline(AutoAdjust adjust) {
        autoAdjust = adjust;
    }

    public AutoAdjust autoAdjust() {
        return autoAdjust;
    }

    @Override public String toString() {
        return "Baseline{" +
            "autoAdjust=" + autoAdjust +
            '}';
    }
}
