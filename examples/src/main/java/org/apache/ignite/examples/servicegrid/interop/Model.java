package org.apache.ignite.examples.servicegrid.interop;

import org.apache.ignite.cache.CacheMode;

public class Model {
    private final int iterationsCount;
    private final String name;
    private final CacheMode cacheMode;
    private Result[] results;

    public Model(String name, int iterationsCount, CacheMode cacheMode) {
        this.name = name;
        this.iterationsCount = iterationsCount;
        this.cacheMode = cacheMode;
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

    public CacheMode getCacheMode() {
        return cacheMode;
    }
}
