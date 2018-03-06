package org.apache.ignite.internal.thinclient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.MarshallerPlatformIds;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryMetadataHandler;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.thinclient.ClientCache;

/**
 * Marshals/unmarshals Ignite Binary Objects.
 * <p>
 * Maintains schema registry to allow deserialization from Ignite Binary format to Java type.
 * </p>
 * <p>
 * The class is a singleton to share the schema registry between different instances of {@link ClientCache}.
 * </p>
 */
enum IgniteBinaryMarshaller {
    /** Singleton. */
    INSTANCE;

    /**
     * Deserializes Ignite binary object from input stream.
     *
     * @return Binary object.
     */
    public <T> T unmarshal(BinaryInputStream in) {
        return impl.unmarshal(in);
    }

    /**
     * Serializes Java object into a byte array.
     */
    public byte[] marshal(Object obj) {
        return impl.marshal(obj);
    }

    /**
     * Configure marshaller with custom Ignite Binary Object configuration.
     */
    public void setBinaryConfiguration(BinaryConfiguration binCfg) {
        if (impl.context().configuration().getBinaryConfiguration() != binCfg)
            impl = createImpl(binCfg);
    }

    /**
     * @return The marshaller context.
     */
    public BinaryContext context() {
        return impl.context();
    }

    /** Marshaller implementation from Ignite core. It maintains local type metadata.w */
    private static GridBinaryMarshaller impl = createImpl(null);

    /** Create new marshaller implementation. */
    private static GridBinaryMarshaller createImpl(BinaryConfiguration binCfg) {
        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        if (binCfg == null) {
            binCfg = new BinaryConfiguration();

            binCfg.setCompactFooter(false);
        }

        igniteCfg.setBinaryConfiguration(binCfg);

        // Use in-memory metadata storage for the thin client
        BinaryMetadataHandler metaHnd = BinaryCachingMetadataHandler.create();

        BinaryContext ctx = new BinaryContext(metaHnd, igniteCfg, new NullLogger());

        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(new LocalMarshallerContext());

        ctx.configure(marsh, igniteCfg);

        ctx.registerUserTypesSchema();

        return new GridBinaryMarshaller(ctx);
    }

    /** In-memory */
    private static class LocalMarshallerContext implements MarshallerContext {
        /** Type ID -> class name map. */
        private Map<Integer, String> types = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override
        public boolean registerClassName(byte platformId, int typeId, String clsName) {
            if (platformId != MarshallerPlatformIds.JAVA_ID)
                throw new IllegalArgumentException("platformId");

            types.put(typeId, clsName);

            return true;
        }

        /** {@inheritDoc} */
        @Override
        public boolean registerClassNameLocally(byte platformId, int typeId, String clsName) {
            return registerClassName(platformId, typeId, clsName);
        }

        /** {@inheritDoc} */
        @Override
        public Class getClass(int typeId, ClassLoader ldr) throws ClassNotFoundException {
            return U.forName(getClassName(MarshallerPlatformIds.JAVA_ID, typeId), ldr, null);
        }

        /** {@inheritDoc} */
        @Override
        public String getClassName(byte platformId, int typeId) throws ClassNotFoundException {
            if (platformId != MarshallerPlatformIds.JAVA_ID)
                throw new IllegalArgumentException("platformId");

            String clsName = types.get(typeId);

            if (clsName == null)
                throw new ClassNotFoundException(String.format("Unknown type id [%s]", typeId));

            return clsName;
        }

        /** {@inheritDoc} */
        @Override public boolean isSystemType(String typeName) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public IgnitePredicate<String> classNameFilter() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public JdkMarshaller jdkMarshaller() {
            return new JdkMarshaller();
        }
    }
}
