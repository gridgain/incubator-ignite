package org.apache.ignite.internal.util.nio.operation;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.util.nio.GridNioBackPressureControl;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridSelectorNioSessionImpl;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.messages.SystemMessage;

/**
 *
 */
public final class SessionWriteRequestImpl implements SessionWriteRequest {
    /** */
    private final Object msg;

    /** */
    private final boolean system;

    /** */
    private final IgniteInClosure<IgniteException> ackC;

    /** Span for tracing. */
    private final Span span;

    /** */
    private GridNioSession ses;

    /** */
    private boolean skipBackPressure;

    /**
     * @param ses Session.
     * @param msg Message.
     * @param ackC Closure invoked when message ACK is received.
     */
    public SessionWriteRequestImpl(GridNioSession ses, Object msg, IgniteInClosure<IgniteException> ackC) {
        this(ses, msg, GridNioBackPressureControl.threadProcessingMessage(), ackC);
    }

    /**
     * @param ses Session.
     * @param msg Message.
     * @param skipBackPressure Skip back pressure flag.
     * @param ackC Closure invoked when message ACK is received.
     */
    public SessionWriteRequestImpl(GridNioSession ses, Object msg, boolean skipBackPressure,
        IgniteInClosure<IgniteException> ackC) {
        this.ses = ses;
        this.msg = msg;
        this.skipBackPressure = skipBackPressure;
        this.ackC = ackC;

        system = msg instanceof SystemMessage;
        span = MTC.span();

        // System messages are unrecoverable, so, no acknowlegement can be received,
        // not-null ack closure with system message points to wrong logic.
        assert !system || ackC == null : "msg=" + msg + ", ackC=" + ackC;
    }

    /** {@inheritDoc} */
    @Override public boolean system() {
        return system;
    }

    /** {@inheritDoc} */
    @Override public boolean skipBackPressure() {
        return skipBackPressure || system;
    }

    /** {@inheritDoc} */
    @Override public GridNioSession session() {
        return ses;
    }

    /** {@inheritDoc} */
    @Override public NioOperation operation() {
        return NioOperation.REQUIRE_WRITE;
    }

    /** {@inheritDoc} */
    @Override public Span span() {
        return span;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        assert msg instanceof Message;

        if (ackC != null)
            ackC.apply(null);

        ((Message)msg).onAckReceived();
    }

    /** {@inheritDoc} */
    @Override public void onMessageWritten() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onError(Exception e) {
        if (ackC != null) {
            IgniteException ex = e instanceof IgniteException
                ? (IgniteException) e
                : new IgniteException(e);

            ackC.apply(ex);
        }
    }

    /** {@inheritDoc} */
    @Override public Object message() {
        return msg;
    }

    /** {@inheritDoc} */
    @Override public SessionWriteRequest recover(GridNioSession ses) {
        assert ses instanceof GridSelectorNioSessionImpl;

        this.ses = ses;
        skipBackPressure = true;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SessionWriteRequestImpl.class, this);
    }
}
