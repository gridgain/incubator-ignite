package org.apache.ignite.configuration.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.internal.property.Modifier;
import org.apache.ignite.configuration.internal.property.NamedList;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class NamedListConfiguration<U, T extends Modifier<U>> extends DynamicConfiguration<NamedList<U>, Object> {
    private final BiFunction<String, String, T> creator;

    Map<String, T> values = new HashMap<>();

    public NamedListConfiguration(String prefix, String key, BiFunction<String, String, T> creator) {
        super(prefix, key);
        this.creator = creator;
    }

    @Override
    public void change(Object o) {

    }

    @Override public void updateValue(String key, Object newValue) {
        String name = key.split("\\.")[1];
        if(!values.containsKey(name))
            values.put(name, add(creator.apply(qualifiedName, name)));

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
