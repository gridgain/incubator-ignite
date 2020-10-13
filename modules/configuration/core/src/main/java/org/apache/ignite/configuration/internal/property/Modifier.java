package org.apache.ignite.configuration.internal.property;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface Modifier<T> {

    String key();

    T toView();

    Modifier<T> find(String key);

    void updateValue(String key, Object newValue);
}
