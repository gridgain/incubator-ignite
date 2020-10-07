package org.apache.ignite.internal.configuration.setpojo;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class NList<T> extends Builder {

    public NList<T> add(String name, T value) {
        changes.put(name, value);

        return this;
    }

    @SafeVarargs
    public static <T> NList<T> list(Pair<T>... pairs){
        NList<T> list = new NList<>();
        for(Pair<T> p : pairs)
            list.add(p.name, p.value);
        return list;
    }

    public static <T extends Builder> Pair<T> pair(String name, T value) {
        return new Pair<>(name, value);
    }

    public static class Pair<T> {
        private final String name;
        private final T value;

        public Pair(String name, T value) {
            this.name = name;
            this.value = value;
        }
    }
}
