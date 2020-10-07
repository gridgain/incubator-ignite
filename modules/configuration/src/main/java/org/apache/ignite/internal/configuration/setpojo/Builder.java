package org.apache.ignite.internal.configuration.setpojo;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.configuration.Keys;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class Builder {
    protected Map<String, Object> changes = new HashMap<>();

    public Map<String, Object> patch() {
        HashMap<String, Object> result = new HashMap<>();

        makePatch("", changes, result);

        return result;
    }

    private void makePatch(String prefix, Map<String, Object> source, Map<String, Object> result) {
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            if (entry.getValue() instanceof Builder) {
                Builder builder = (Builder)entry.getValue();

                makePatch(Keys.concat(prefix, entry.getKey()), builder.changes, result);
            }
            else
                result.put(Keys.concat(prefix, entry.getKey()), entry.getValue());
        }
    }
}
