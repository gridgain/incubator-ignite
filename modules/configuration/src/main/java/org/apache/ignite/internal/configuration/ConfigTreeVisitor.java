package org.apache.ignite.internal.configuration;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.configuration.internalconfig.DynamicConfiguration;
import org.apache.ignite.internal.configuration.internalconfig.DynamicProperty;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class ConfigTreeVisitor {
    Map<String, Modifier> map = new HashMap<>();

    public void visit(String path, DynamicProperty dynamicProperty) {
        map.put(Keys.concat(path, dynamicProperty.key()), dynamicProperty);
    }

    public void visit(String path, DynamicConfiguration dynamicConfiguration) {
        String concat = Keys.concat(path, dynamicConfiguration.key());

        map.put(concat, dynamicConfiguration);

//        dynamicConfiguration.accept(concat, this);
    }

    public Map<String, Modifier> result() {
        return map;
    }
}
