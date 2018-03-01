package org.apache.ignite.client.system;

import org.apache.ignite.*;
import org.apache.ignite.binary.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.client.*;
import org.apache.ignite.configuration.*;
import org.junit.*;

import javax.cache.*;
import java.util.*;
import java.util.stream.*;

import static org.junit.Assert.*;

/**
 * Ignite {@link BinaryObject} API system tests.
 */
public class IgniteBinaryTest {
    /**
     * Binary configuration allows unmarshalling schema-less Ignite binary objects into Java static types.
     */
    @Test
    public void testBinaryConfigurationAllowsUnmarshalSchemalessIgniteBinaries() throws Exception {
        int key = 1;
        Person val = new Person(key, "Joe");

        try (Ignite srv = Ignition.start(Config.getServerConfiguration())) {
            // Add an entry directly to the Ignite server. This stores a schema-less object in the cache and also
            // does not register schema in the client. Thus, unmarshalling this value to Java type would not work
            // without binary configuration.
            srv.cache(Config.DEFAULT_CACHE_NAME).put(key, val);

            try (IgniteClient client = IgniteClient.start(getClientConfiguration(true))) {
                CacheClient<Integer, Person> cache = client.cache(Config.DEFAULT_CACHE_NAME);

                Person cachedVal = cache.get(key);

                assertEquals(val, cachedVal);
            }
        }
    }

    /**
     * Binary configuration allows reading schema-less Ignite Binary object.
     */
    @Test
    public void testBinaryConfigurationAllowsReadingSchemalessIgniteBinaries() throws Exception {
        int key = 1;
        Person val = new Person(key, "Joe");

        try (Ignite srv = Ignition.start(Config.getServerConfiguration())) {
            // Add an entry directly to the Ignite server. This stores a schema-less object in the cache and also
            // does not register schema in the client. Thus, reading this value in Ignite Binary format would not work
            // without binary configuration.
            srv.cache(Config.DEFAULT_CACHE_NAME).put(key, val);

            try (IgniteClient client = IgniteClient.start(getClientConfiguration(true))) {
                CacheClient<Integer, BinaryObject> cache = client.cache(Config.DEFAULT_CACHE_NAME).withKeepBinary();

                BinaryObject cachedVal = cache.get(key);

                assertEquals(val.getId(), cachedVal.field("id"));
                assertEquals(val.getName(), cachedVal.field("name"));
            }
        }
    }

    /**
     * Put/get operations with Ignite Binary Object API
     */
    @Test
    public void testBinaryObjectPutGet() throws Exception {
        int key = 1;

        try (Ignite ignored = Ignition.start(Config.getServerConfiguration())) {
            try (IgniteClient client = IgniteClient.start(getClientConfiguration(false))) {
                IgniteBinary binary = client.binary();

                BinaryObject val = binary.builder("Person")
                    .setField("id", 1, int.class)
                    .setField("name", "Joe", String.class)
                    .build();

                CacheClient<Integer, BinaryObject> cache = client.cache(Config.DEFAULT_CACHE_NAME).withKeepBinary();

                cache.put(key, val);

                BinaryObject cachedVal =
                    client.cache(Config.DEFAULT_CACHE_NAME).<Integer, BinaryObject>withKeepBinary().get(key);

                assertBinaryObjectsEqual(val, cachedVal);
            }
        }
    }

    /**
     * Binary Object API:
     * {@link IgniteBinary#typeId(String)}
     * {@link IgniteBinary#toBinary(Object)}
     * {@link IgniteBinary#type(int)}
     * {@link IgniteBinary#type(Class)}
     * {@link IgniteBinary#type(String)}
     * {@link IgniteBinary#types()}
     * {@link IgniteBinary#buildEnum(String, int)}
     * {@link IgniteBinary#buildEnum(String, String)}
     * {@link IgniteBinary#registerEnum(String, Map)}
     */
    @Test
    public void testBinaryObjectApi() throws Exception {
        try (Ignite srv = Ignition.start(Config.getServerConfiguration())) {
            try (IgniteClient client = IgniteClient.start(getClientConfiguration(false))) {
                // Use "server-side" IgniteBinary as a reference to test the thin client IgniteBinary against
                IgniteBinary refBinary = srv.binary();

                IgniteBinary binary = client.binary();

                Person obj = new Person(1, "Joe");

                int refTypeId = refBinary.typeId(Person.class.getName());
                int typeId = binary.typeId(Person.class.getName());

                assertEquals(refTypeId, typeId);

                BinaryObject refBinObj = refBinary.toBinary(obj);
                BinaryObject binObj = binary.toBinary(obj);

                assertBinaryObjectsEqual(refBinObj, binObj);

                assertBinaryTypesEqual(refBinary.type(typeId), binary.type(typeId));

                assertBinaryTypesEqual(refBinary.type(Person.class), binary.type(Person.class));

                assertBinaryTypesEqual(refBinary.type(Person.class.getName()), binary.type(Person.class.getName()));

                Collection<BinaryType> refTypes = refBinary.types();
                Collection<BinaryType> types = binary.types();

                assertEquals(refTypes.size(), types.size());

                BinaryObject refEnm = refBinary.buildEnum(Enum.class.getName(), Enum.DEFAULT.ordinal());
                BinaryObject enm = binary.buildEnum(Enum.class.getName(), Enum.DEFAULT.ordinal());

                assertBinaryObjectsEqual(refEnm, enm);

                Map<String, Integer> enumMap = Arrays.stream(Enum.values())
                    .collect(Collectors.toMap(java.lang.Enum::name, java.lang.Enum::ordinal));

                BinaryType refEnumType = refBinary.registerEnum(Enum.class.getName(), enumMap);
                BinaryType enumType = binary.registerEnum(Enum.class.getName(), enumMap);

                assertBinaryTypesEqual(refEnumType, enumType);

                refEnm = refBinary.buildEnum(Enum.class.getName(), Enum.DEFAULT.name());
                enm = binary.buildEnum(Enum.class.getName(), Enum.DEFAULT.name());

                assertBinaryObjectsEqual(refEnm, enm);
            }
        }
    }

    /**
     * Test queries in Ignite binary.
     */
    @Test
    public void testBinaryQueries() throws Exception {
        try (Ignite ignored = Ignition.start(Config.getServerConfiguration());
             IgniteClient client = IgniteClient.start(new IgniteClientConfiguration(Config.HOST))
        ) {
            final String TYPE_NAME = "Person";

            QueryEntity qryPerson = new QueryEntity();

            qryPerson.setKeyType(Integer.class.getName());
            qryPerson.setValueType(TYPE_NAME);

            Collection<QueryField> fields = new ArrayList<>();

            fields.add(new QueryField("id", Integer.class.getName()));
            fields.add(new QueryField("name", String.class.getName()));

            qryPerson.setFields(fields);

            qryPerson.setIndexes(Collections.singletonList(new QueryIndex("id")));

            CacheClientConfiguration cacheCfg = new CacheClientConfiguration("testBinaryQueries")
                .setQueryEntities(Collections.singletonList(qryPerson));

            CacheClient<Integer, BinaryObject> cache = client.getOrCreateCache(cacheCfg).withKeepBinary();

            final IgniteBinary binary = client.binary();

            Map<Integer, BinaryObject> data = IntStream.range(0, 100).boxed().collect(Collectors.toMap(
                i -> i,
                i ->
                    binary.builder(TYPE_NAME)
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
    private static IgniteClientConfiguration getClientConfiguration(boolean withBinaryCfg) {
        IgniteClientConfiguration clientCfg = new IgniteClientConfiguration(Config.HOST);

        if (withBinaryCfg) {
            BinaryConfiguration binCfg = new BinaryConfiguration();

            BinaryTypeConfiguration personCfg = new BinaryTypeConfiguration("org.apache.ignite.client.Person");

            binCfg.setTypeConfigurations(Collections.singletonList(personCfg));

            clientCfg.setBinaryConfiguration(binCfg);
        }

        return clientCfg;
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

    /**
     * Enumeration for tests.
     */
    private enum Enum {
        /** Default. */DEFAULT
    }
}
