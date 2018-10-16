package org.apache.ignite.examples.servicegrid.interop;

public class Model {
    private final int iterationsCount;
    private final String name;
    private Result[] results;

    public Model(String name, int iterationsCount) {
        this.name = name;
        this.iterationsCount = iterationsCount;
    }

    public int getIterationsCount() {
        return iterationsCount;
    }

    public String getName() {
        return name;
    }

    public Result[] getResults() {
        return results;
    }

    public void setResults(Result[] results) {
        this.results = results;
    }
}
