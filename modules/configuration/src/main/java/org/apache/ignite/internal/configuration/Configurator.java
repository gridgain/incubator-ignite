package org.apache.ignite.internal.configuration;

import org.apache.ignite.internal.configuration.internalconfig.LocalConfiguration;
import org.apache.ignite.internal.configuration.selector.Selector;
import org.apache.ignite.internal.configuration.setpojo.Builder;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class Configurator {

    LocalConfiguration local = new LocalConfiguration();

    public Configurator() {
        LocalConfiguration local = new LocalConfiguration();

//        ConfigTreeVisitor visitor = new ConfigTreeVisitor();
//
//        local.accept("", visitor);
//
//        this.map = visitor.result();
    }

    public <O, C, I, In> O getPublic(Selector<O, C, I, In> selector) {
        return (O)(local.find(selector.key()).toView());
    }

    public <O, C, I, In> void set(Selector<O, C, I, In> selector, C newValue) {
        if(newValue instanceof Builder)
            ((Builder)newValue).patch().forEach((key, value) -> local.updateValue(Keys.concat(selector.key(), key), value));
        else
            local.updateValue(selector.key(), newValue);
    }

    public <O, C, I, In> void init(Selector<O, C, I, In> selector, I newValue) {
        if(newValue instanceof Builder)
            ((Builder)newValue).patch().forEach((key, value) -> local.updateValue(Keys.concat(selector.key(), key), value));
        else
            local.updateValue(selector.key(), newValue);
    }

    public <O, C, I, In> In getInternal(Selector<O, C, I, In> selector) {
        return (In)(local.find(selector.key()));
    }

}
