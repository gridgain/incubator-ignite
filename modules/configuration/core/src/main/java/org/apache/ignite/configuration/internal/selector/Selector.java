package org.apache.ignite.configuration.internal.selector;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class Selector<OutputT, ChangeT, InitT, InnerT> {
    private final String key;
//    private final Class<T> type;

    public Selector(String key) {
        this.key = key;
//        this.type = type;
    }

    public String key() {
        return key;
    }
}
