package org.apache.ignite.internal.configuration.internalconfig;

import java.util.Map;
import org.apache.ignite.internal.configuration.ConfigTreeVisitor;
import org.apache.ignite.internal.configuration.Modifier;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class DynamicConfiguration<T> implements Modifier<T> {
    private final String key;

    protected DynamicConfiguration(String key) {
        this.key = key;
    }

    @Override public void updateValue(Object newValue) {
        if (newValue instanceof Map)
            updateValue(((Map<String, Object>)newValue));
    }

    abstract void updateValue(Map<String, Object> map);

    abstract public void accept(String path, ConfigTreeVisitor visitor);

    public String key() {
        return key;
    }
}
