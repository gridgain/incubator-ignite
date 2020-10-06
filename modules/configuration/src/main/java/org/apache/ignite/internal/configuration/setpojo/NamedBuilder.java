package org.apache.ignite.internal.configuration.setpojo;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class NamedBuilder extends Builder{
    private final String name;

    public NamedBuilder(String name) {
        this.name = name;
    }

    public String name() {
        return name;
    }
}
