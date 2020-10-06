package org.apache.ignite.internal.configuration.setpojo;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class Builder {
    protected Map<String, Object> changes = new HashMap<>();

    public Map<String, Object> changes() {
        return changes;
    }
}
