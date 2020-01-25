package org.apache.ignite.internal.processors.query.schema;

import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class SchemaIndexCacheStat {
    public Set<String> types = new HashSet<>(); // Indexed types.
    public int scanned; // Indexed keys.
}
