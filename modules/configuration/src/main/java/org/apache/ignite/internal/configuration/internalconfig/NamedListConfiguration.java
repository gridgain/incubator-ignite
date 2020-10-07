package org.apache.ignite.internal.configuration.internalconfig;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.configuration.Modifier;
import org.apache.ignite.internal.configuration.getpojo.NamedList;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class NamedListConfiguration<U, T extends Modifier<U>> extends DynamicConfiguration<NamedList<U>> {
    private final Function<String, T>  creator;

    Map<String, T> values = new HashMap<>();

    public NamedListConfiguration(String key, Function<String, T> creator) {
        super(key);
        this.creator = creator;
    }

    @Override public void updateValue(String key, Object newValue) {
        String name = key.split("\\.")[1];
        if(!values.containsKey(name))
            values.put(name, add(creator.apply(name)));

        super.updateValue(key, newValue);
    }

    @Override public NamedList<U> toView() {
        return new NamedList<>(values.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, it-> it.getValue().toView())));
    }

//    public void accept(String path, ConfigTreeVisitor visitor) {
//        visitor.visit(path, this);
////        timeout.accept(path, visitor);
////        enabled.accept(path, visitor);
//    }
}
