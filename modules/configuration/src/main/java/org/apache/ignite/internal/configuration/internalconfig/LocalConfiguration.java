package org.apache.ignite.internal.configuration.internalconfig;

import org.apache.ignite.internal.configuration.getpojo.Local;

import static org.apache.ignite.internal.configuration.Keys.LOCAL;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class LocalConfiguration extends DynamicConfiguration<Local> {
    private final BaselineConfiguration baselineConfiguration = add(new BaselineConfiguration());

    public LocalConfiguration() {
        super(LOCAL);
    }

    public BaselineConfiguration baselineConfiguration() {
        return baselineConfiguration;
    }

    @Override public Local toView() {
        return new Local(baselineConfiguration.toView());
    }

//    public void accept(String path, ConfigTreeVisitor visitor) {
//        visitor.visit(path, this);
//
//        path = concat(path, key());
//
//        baselineConfiguration.accept(path, visitor);
//    }
}
