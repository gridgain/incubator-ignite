package org.apache.ignite.examples.servicegrid.interop;

public class Result {
    private final String name;
    private final double value;

    public Result(String name, double value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public double getValue() {
        return value;
    }
}
