package org.apache.ignite.internal.configuration.internalconfig;

import java.util.Map;
import org.apache.ignite.internal.configuration.ConfigTreeVisitor;
import org.apache.ignite.internal.configuration.getpojo.Local;
import org.apache.ignite.internal.configuration.setpojo.Builder;

import static org.apache.ignite.internal.configuration.Keys.LOCAL;
import static org.apache.ignite.internal.configuration.Keys.concat;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class LocalConfiguration extends DynamicConfiguration<Local> {
    private final BaselineConfiguration baselineConfiguration = new BaselineConfiguration();

    public LocalConfiguration() {
        super(LOCAL);
    }

    public BaselineConfiguration baselineConfiguration() {
        return baselineConfiguration;
    }

    @Override public Local toView() {
        return new Local(baselineConfiguration.toView());
    }

    @Override public void updateValue(Map<String, Object> map) {
        if (map.containsKey(LOCAL))
            baselineConfiguration.updateValue(((Builder)map.get(LOCAL)).changes());

    }

    public void accept(String path, ConfigTreeVisitor visitor) {
        visitor.visit(path, this);

        path = concat(path, key());

        baselineConfiguration.accept(path, visitor);
    }
}
