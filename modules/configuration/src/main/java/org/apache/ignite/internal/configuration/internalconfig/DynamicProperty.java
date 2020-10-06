package org.apache.ignite.internal.configuration.internalconfig;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import com.sun.istack.internal.NotNull;
import org.apache.ignite.internal.configuration.ConfigTreeVisitor;
import org.apache.ignite.internal.configuration.Modifier;

import static java.util.Objects.requireNonNull;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class DynamicProperty<T> implements Modifier<T> {
    /** Name of property. */
    private final String name;

    /** Property value. */
    protected volatile T val;

    /** Listeners of property update. */
    private final ConcurrentLinkedQueue<DynamicProperty<? super T>> updateListeners = new ConcurrentLinkedQueue<>();

    public DynamicProperty(String name, @NotNull T defaultValue) {
        this.name = name;
        this.val = requireNonNull(defaultValue);
    }

    public T value() {
        return val;
    }

    @Override public T toView() {
        return val;
    }

    @Override public void updateValue(Object object) {
        if (object instanceof Map) {
            Map<String, Object> values = (Map<String, Object>) object;
            if(values.containsKey(name))
                val = (T)values.get(name);
        } else {
            if(object != null)
                val = (T)object;
        }

    }

    public String key() {
        return name;
    }

    public void accept(String path, ConfigTreeVisitor visitor) {
        visitor.visit(path, this);
    }
}
