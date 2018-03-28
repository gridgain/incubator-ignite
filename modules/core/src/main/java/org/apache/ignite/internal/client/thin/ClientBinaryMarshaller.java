package org.apache.ignite.internal.client.thin;

import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryMetadataHandler;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.marshaller.MarshallerContext;

/**
 * Marshals/unmarshals Ignite Binary Objects.
 * <p>
 * Maintains schema registry to allow deserialization from Ignite Binary format to Java type.
 * </p>
 */
class ClientBinaryMarshaller {
    /** Metadata handler. */
    private final BinaryMetadataHandler metaHnd;

    /** Marshaller context. */
    private final MarshallerContext marshCtx;

    /** Re-using marshaller implementation from Ignite core. */
    private GridBinaryMarshaller impl;

    /**
     * Constructor.
     */
    ClientBinaryMarshaller(BinaryMetadataHandler metaHnd, MarshallerContext marshCtx) {
        this.metaHnd = metaHnd;
        this.marshCtx = marshCtx;

        impl = createImpl(null);
    }

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

    /** Create new marshaller implementation. */
    private GridBinaryMarshaller createImpl(BinaryConfiguration binCfg) {
        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        if (binCfg == null) {
            binCfg = new BinaryConfiguration();

            binCfg.setCompactFooter(false);
        }

        igniteCfg.setBinaryConfiguration(binCfg);

        BinaryContext ctx = new BinaryContext(metaHnd, igniteCfg, new NullLogger());

        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(marshCtx);

        ctx.configure(marsh, igniteCfg);

        ctx.registerUserTypesSchema();

        return new GridBinaryMarshaller(ctx);
    }
}

