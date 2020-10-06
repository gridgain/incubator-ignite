package org.apache.ignite.internal.configuration;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.configuration.internalconfig.LocalConfiguration;
import org.apache.ignite.internal.configuration.selector.Selector;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class Configurator {

    Map<String, Modifier> map = new HashMap<>();

    public Configurator() {
        LocalConfiguration local = new LocalConfiguration();

        ConfigTreeVisitor visitor = new ConfigTreeVisitor();

        local.accept("", visitor);

        this.map = visitor.result();
    }

    public <O, C, I, In> O getPublic(Selector<O, C, I, In> selector) {
        return (O)(map.get(selector.key()).toView());
    }

    public <O, C, I, In> void set(Selector<O, C, I, In> selector, C newValue) {
        Map<String, Object> wrap = new HashMap<>();
        wrap.put(selector.key(), newValue);

        map.get(selector.key()).updateValue(wrap);
    }

    public <O, C, I, In> void init(Selector<O, C, I, In> selector, I newValue) {
        Map<String, Object> wrap = new HashMap<>();
        wrap.put(selector.key(), newValue);

        map.get(selector.key()).updateValue(wrap);
    }

    public <O, C, I, In> In getInternal(Selector<O, C, I, In> selector) {
        return (In)(map.get(selector.key()));
    }

}
