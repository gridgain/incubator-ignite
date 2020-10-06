package org.apache.ignite.internal.configuration;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface Modifier<T> {

    T toView();

    void updateValue(Object newValue);
}
