package org.apache.ignite.internal.configuration.internalconfig;

import org.apache.ignite.internal.configuration.getpojo.Baseline;
import org.apache.ignite.internal.configuration.getpojo.Node;

import static org.apache.ignite.internal.configuration.Keys.BASELINE;
import static org.apache.ignite.internal.configuration.Keys.NODE;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class BaselineConfiguration extends DynamicConfiguration<Baseline> {
    private final AutoAdjustConfiguration autoAdjustConfiguration = add(new AutoAdjustConfiguration());

    private final NamedListConfiguration<Node, NodeConfiguration> nodes = add(new NamedListConfiguration<>(NODE, NodeConfiguration::new));

    public BaselineConfiguration() {
        super(BASELINE);
    }

    public AutoAdjustConfiguration autoAdjustConfiguration() {
        return autoAdjustConfiguration;
    }

    @Override public Baseline toView() {
        return new Baseline(autoAdjustConfiguration.toView());
    }

//    public void accept(String path, ConfigTreeVisitor visitor) {
//        visitor.visit(path, this);
//
//        path = concat(path, key());
//
//        autoAdjustConfiguration.accept(path, visitor);
//        nodes.accept(path, visitor);
//    }
}
