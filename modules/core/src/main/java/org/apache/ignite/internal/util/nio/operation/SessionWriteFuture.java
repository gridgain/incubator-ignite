package org.apache.ignite.internal.util.nio.operation;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.util.nio.GridNioBackPressureControl;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.GridSelectorNioSessionImpl;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.messages.SystemMessage;

public class SessionWriteFuture extends SessionOperationFuture<Void> implements SessionWriteRequest{
    /** */
    private final Object msg;

    /** */
    private final boolean system;

    /** */
    private final IgniteInClosure<IgniteException> ackC;

    /** */
    private final Span span;

    /** */
    private boolean skipBackPressure;

    /**
     * @param ses Session.
     * @param msg Message.
     * @param ackC Closure invoked when message ACK is received.
     */
    public SessionWriteFuture(GridSelectorNioSessionImpl ses, Object msg, IgniteInClosure<IgniteException> ackC) {
        super(ses, NioOperation.REQUIRE_WRITE);
        this.msg = msg;
        this.ackC = ackC;

        system = msg instanceof SystemMessage;
        skipBackPressure = system || GridNioBackPressureControl.threadProcessingMessage();
        span = MTC.span();

        // System messages are unrecoverable, so, no acknowlegement can be received,
        // not-null ack closure with system message points to wrong logic.
        assert !system || ackC == null : "msg=" + msg + ", ackC=" + ackC;
    }

    /** {@inheritDoc} */
    @Override public boolean skipBackPressure() {
        return skipBackPressure || system;
    }

    /** {@inheritDoc} */
    @Override public boolean system() {
        return system;
    }

    /** {@inheritDoc} */
    @Override public Span span() {
        return span;
    }

    /** {@inheritDoc} */
    @Override public Object message() {
        return msg;
    }

    /** {@inheritDoc} */
    @Override public void onMessageWritten() {
        onDone();
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        assert msg instanceof Message;

        if (ackC != null)
            ackC.apply(null);

        ((Message)msg).onAckReceived();
    }

    /** {@inheritDoc} */
    @Override public void onError(Exception e) {
        super.onError(e);

        if (ackC != null) {
            IgniteException ex = e instanceof IgniteException
                ? (IgniteException) e
                : new IgniteException(e);

            ackC.apply(ex);
        }
    }

    /** {@inheritDoc} */
    @Override public SessionWriteRequest recover(GridNioSession ses) {
        assert ses instanceof GridSelectorNioSessionImpl;

        this.ses = (GridSelectorNioSessionImpl)ses;
        skipBackPressure = true;

        return this;
    }
}
