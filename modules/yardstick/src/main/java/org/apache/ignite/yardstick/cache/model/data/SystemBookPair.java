package org.apache.ignite.yardstick.cache.model.data;

public class SystemBookPair {

    final private String system;

    final private String book;

    public SystemBookPair(String system, String book) {
        this.system = system;
        this.book = book;
    }

    public String getSystem() {
        return system;
    }

    public String getBook() {
        return book;
    }
}
