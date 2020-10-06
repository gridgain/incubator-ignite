package org.apache.ignite.internal.configuration.internalconfig;

import java.util.Map;
import org.apache.ignite.internal.configuration.ConfigTreeVisitor;
import org.apache.ignite.internal.configuration.getpojo.Baseline;
import org.apache.ignite.internal.configuration.getpojo.Node;
import org.apache.ignite.internal.configuration.setpojo.Builder;

import static org.apache.ignite.internal.configuration.Keys.BASELINE;
import static org.apache.ignite.internal.configuration.Keys.NODE;
import static org.apache.ignite.internal.configuration.Keys.concat;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class BaselineConfiguration extends DynamicConfiguration<Baseline> {
    private final AutoAdjustConfiguration autoAdjustConfiguration = new AutoAdjustConfiguration();

    private final NamedListConfiguration<Node, NodeConfiguration> nodes = new NamedListConfiguration<>(NODE, NodeConfiguration::new);

    public BaselineConfiguration() {
        super(BASELINE);
    }

    public AutoAdjustConfiguration autoAdjustConfiguration() {
        return autoAdjustConfiguration;
    }

    @Override public Baseline toView() {
        return new Baseline(autoAdjustConfiguration.toView());
    }

    @Override public void updateValue(Map<String, Object> newValue) {
        if(newValue.containsKey(key())) {
            Map<String, Object> changes = ((Builder)newValue.get(key())).changes();
            autoAdjustConfiguration.updateValue(changes);

            nodes.updateValue(changes);
        }
    }

    public void accept(String path, ConfigTreeVisitor visitor) {
        visitor.visit(path, this);

        path = concat(path, key());

        autoAdjustConfiguration.accept(path, visitor);
        nodes.accept(path, visitor);
    }
}
