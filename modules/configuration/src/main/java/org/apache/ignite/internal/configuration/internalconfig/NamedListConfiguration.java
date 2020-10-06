package org.apache.ignite.internal.configuration.internalconfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.configuration.ConfigTreeVisitor;
import org.apache.ignite.internal.configuration.Modifier;
import org.apache.ignite.internal.configuration.getpojo.NamedList;
import org.apache.ignite.internal.configuration.setpojo.NamedBuilder;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class NamedListConfiguration<U, T extends Modifier<U>> extends DynamicConfiguration<NamedList<U>> {
    private final String key;

    private final Supplier<T> creator;

    Map<String, T> values = new HashMap<>();

    public NamedListConfiguration(String key, Supplier<T> creator) {
        super(key);
        this.key = key;
        this.creator = creator;
    }

    @Override void updateValue(Map<String, Object> map) {
        if (map.containsKey(key)) {
            List<NamedBuilder> list = (List<NamedBuilder>)map.get(key);

            list.forEach(b -> {
                    Map<String, Object> wrap = new HashMap<>();
                    wrap.put(key, b);

                    values.computeIfAbsent(b.name(), k -> creator.get()).updateValue(wrap);
                }
            );
        }
    }

    @Override public NamedList<U> toView() {
        return new NamedList<>(values.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, it-> it.getValue().toView())));
    }

    public void accept(String path, ConfigTreeVisitor visitor) {
        visitor.visit(path, this);
//        timeout.accept(path, visitor);
//        enabled.accept(path, visitor);
    }
}
