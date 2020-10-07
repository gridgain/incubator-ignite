package org.apache.ignite.internal.configuration.internalconfig;

import org.apache.ignite.internal.configuration.getpojo.AutoAdjust;

import static org.apache.ignite.internal.configuration.Keys.AUTO_ADJUST;
import static org.apache.ignite.internal.configuration.Keys.ENABLED;
import static org.apache.ignite.internal.configuration.Keys.TIMEOUT;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class AutoAdjustConfiguration extends DynamicConfiguration<AutoAdjust> {
    private final DynamicProperty<Long> timeout = add(new DynamicProperty<>(TIMEOUT, 5000L));

    private final DynamicProperty<Boolean> enabled = add(new DynamicProperty<>(ENABLED, false));

    public AutoAdjustConfiguration() {
        super(AUTO_ADJUST);
    }

    public DynamicProperty<Long> timeout() {
        return timeout;
    }

    public DynamicProperty<Boolean> enabled() {
        return enabled;
    }

    @Override public AutoAdjust toView() {
        return new AutoAdjust(timeout.toView(), enabled.toView());
    }

//    public void accept(String path, ConfigTreeVisitor visitor) {
//        visitor.visit(path, this);
//
//        path = concat(path, key());
//
//        timeout.accept(path, visitor);
//        enabled.accept(path, visitor);
//    }
}
