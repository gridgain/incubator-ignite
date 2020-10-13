package org.apache.ignite.configuration.internal.property;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class DynamicProperty<T> implements Modifier<T> {
    /** Name of property. */
    private final String name;

    /** Full name with prefix. */
    private final String qualifiedName;

    /** Property value. */
    protected volatile T val;

    /** Listeners of property update. */
    private final ConcurrentLinkedQueue<DynamicProperty<? super T>> updateListeners = new ConcurrentLinkedQueue<>();

    public DynamicProperty(String prefix, String name) {
        this(prefix, name, null);
    }

    public DynamicProperty(String prefix, String name, T defaultValue) {
        this.name = name;
        this.qualifiedName = String.format("%s.%s", prefix, name);
        this.val = defaultValue;
    }

    public T value() {
        return val;
    }

    @Override public T toView() {
        return val;
    }

    @Override public Modifier<T> find(String key) {
        if (key.equals(name))
            return this;

        return null;
    }

    public void change(T object) {
        this.val = object;
    }

    @Override public void updateValue(String key, Object object) {
        if (!name.equals(key))
            throw new IllegalArgumentException();

        val = (T)object;
    }

    @Override public String key() {
        return name;
    }

    public String qualifiedName() {
        return qualifiedName;
    }

    public void accept(String path, ConfigTreeVisitor visitor) {
        visitor.visit(path, this);
    }
}
