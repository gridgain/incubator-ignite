package org.apache.ignite.internal.configuration.setpojo;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.configuration.selector.Selector;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class Patch {
    protected Map<String, Object> changes = new HashMap<>();

    public <O, C, I, In> O getPublic(Selector<O, C, I, In> selector) {
        return (O)(changes.get(selector.key()));
    }
}
