package org.apache.ignite.internal.configuration.internalconfig;

import java.util.Map;
import org.apache.ignite.internal.configuration.ConfigTreeVisitor;
import org.apache.ignite.internal.configuration.getpojo.AutoAdjust;
import org.apache.ignite.internal.configuration.setpojo.Builder;

import static org.apache.ignite.internal.configuration.Keys.AUTO_ADJUST;
import static org.apache.ignite.internal.configuration.Keys.ENABLED;
import static org.apache.ignite.internal.configuration.Keys.TIMEOUT;
import static org.apache.ignite.internal.configuration.Keys.concat;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class AutoAdjustConfiguration extends DynamicConfiguration<AutoAdjust> {
    private final DynamicProperty<Long> timeout = new DynamicProperty<>(TIMEOUT, 5000L);

    private final DynamicProperty<Boolean> enabled = new DynamicProperty<>(ENABLED, false);

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

    @Override public void updateValue(Map<String, Object> map) {
        if (map.containsKey(AUTO_ADJUST)) {
            Map<String, Object> autoAdjustChanges = ((Builder)map.get(AUTO_ADJUST)).changes();

            timeout.updateValue(autoAdjustChanges);
            enabled.updateValue(autoAdjustChanges);
        }
    }

    public void accept(String path, ConfigTreeVisitor visitor) {
        visitor.visit(path, this);

        path = concat(path, key());

        timeout.accept(path, visitor);
        enabled.accept(path, visitor);
    }
}
