package org.apache.ignite.client.system;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Ignite {@link BinaryObject} API system tests.
 */
public class IgniteBinaryQueryTest {
    /**
     * Test queries in Ignite binary.
     */
    @Test
    public void testBinaryQueries() throws Exception {
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))
        ) {
            final String TYPE_NAME = "Person";

            QueryEntity qryPerson = new QueryEntity()
                .setKeyType(Integer.class.getName())
                .setValueType(TYPE_NAME)
                .setFields(
                    Stream.of(
                        new SimpleEntry<>("id", Integer.class.getName()),
                        new SimpleEntry<>("name", String.class.getName())
                    ).collect(Collectors.toMap(
                        SimpleEntry::getKey, SimpleEntry::getValue, (a, b) -> a, LinkedHashMap::new
                    ))
                )
                .setIndexes(Collections.singletonList(new QueryIndex("id")));

            ClientCacheConfiguration cacheCfg = new ClientCacheConfiguration().setName("testBinaryQueries")
                .setQueryEntities(qryPerson);

            ClientCache<Integer, BinaryObject> cache = client.getOrCreateCache(cacheCfg).withKeepBinary();

            final IgniteBinary binary = client.binary();

            Map<Integer, BinaryObject> data = IntStream.range(0, 100).boxed().collect(Collectors.toMap(
                i -> i,
                i -> binary.builder(TYPE_NAME)
                    .setField("id", i, int.class)
                    .setField("name", String.format("Person %s", i), String.class)
                    .build()
            ));

            cache.putAll(data);

            Cache.Entry<Integer, BinaryObject> p1 = cache
                .query(new SqlQuery<Integer, BinaryObject>(TYPE_NAME, "id = 1"))
                .getAll()
                .iterator()
                .next();

            assertEquals(Integer.valueOf(1), p1.getKey());

            assertBinaryObjectsEqual(data.get(1), p1.getValue());
        }
    }

    /** */
    private void assertBinaryTypesEqual(BinaryType exp, BinaryType actual) {
        assertEquals(exp.typeId(), actual.typeId());
        assertEquals(exp.typeName(), actual.typeName());
        assertEquals(exp.fieldNames(), actual.fieldNames());

        for (String f : exp.fieldNames())
            assertEquals(exp.fieldTypeName(f), actual.fieldTypeName(f));

        assertEquals(exp.affinityKeyFieldName(), actual.affinityKeyFieldName());
        assertEquals(exp.isEnum(), actual.isEnum());
    }

    /** */
    private void assertBinaryObjectsEqual(BinaryObject exp, BinaryObject actual) throws Exception {
        assertBinaryTypesEqual(exp.type(), actual.type());

        for (String f : exp.type().fieldNames()) {
            Object expVal = exp.field(f);

            Class<?> cls = expVal.getClass();

            if (cls.getMethod("equals", Object.class).getDeclaringClass() == cls)
                assertEquals(expVal, actual.field(f));
        }

        if (exp.type().isEnum())
            assertEquals(exp.enumOrdinal(), actual.enumOrdinal());
    }
}
