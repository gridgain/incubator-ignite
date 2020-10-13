package org.apache.ignite.configuration.internal.property;

import java.util.Map;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class NamedList<T> {
    private final Map<String, T> list;

    public NamedList(Map<String, T> list) {
        this.list = list;
    }

    @Override public String toString() {
        return "NamedList{" +
            "list=" + list +
            '}';
    }
}
