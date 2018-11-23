package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.internal.util.GridBoundedLinkedHashSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;

/**
 * Pending messages container.
 */
class PendingMessages implements Iterable<TcpDiscoveryAbstractMessage> {
    /** */
    private static final int MAX = 1024;

    /** Pending messages. */
    final Queue<PendingMessage> msgs = new ArrayDeque<>(MAX * 2);

    /** Processed custom message IDs. */
    Set<IgniteUuid> procCustomMsgs = new GridBoundedLinkedHashSet<>(MAX * 2);

    /** Discarded message ID. */
    IgniteUuid discardId;

    /** Discarded custom message ID. */
    IgniteUuid customDiscardId;

    /**
     * Adds pending message and shrinks queue if it exceeds limit
     * (messages that were not discarded yet are never removed).
     *
     * @param msg Message to add.
     */
    void add(TcpDiscoveryAbstractMessage msg) {
        msgs.add(new PendingMessage(msg));

        while (msgs.size() > MAX) {
            PendingMessage polled = msgs.poll();

            assert polled != null;

            if (polled.id.equals(discardId))
                break;
        }
    }

    /**
     * Resets pending messages.
     *
     * @param msgs Message.
     * @param discardId Discarded message ID.
     * @param customDiscardId Discarded custom event message ID.
     */
    void reset(
        @Nullable Collection<TcpDiscoveryAbstractMessage> msgs,
        @Nullable IgniteUuid discardId,
        @Nullable IgniteUuid customDiscardId
    ) {
        this.msgs.clear();

        if (msgs != null) {
            for (TcpDiscoveryAbstractMessage msg : msgs)
                this.msgs.add(new PendingMessage(msg));
        }

        this.discardId = discardId;
        this.customDiscardId = customDiscardId;
    }

    /**
     * Discards message with provided ID and all before it.
     *
     * @param id Discarded message ID.
     * @param custom {@code True} if discard for {@link TcpDiscoveryCustomEventMessage}.
     */
    void discard(IgniteUuid id, boolean custom) {
        if (custom)
            customDiscardId = id;
        else
            discardId = id;

        cleanup();
    }

    /**
     *
     */
    void cleanup() {
        Iterator<PendingMessage> msgIt = msgs.iterator();

        boolean skipMsg = discardId != null;
        boolean skipCustomMsg = customDiscardId != null;

        while (msgIt.hasNext()) {
            PendingMessage msg = msgIt.next();

            if (msg.customMsg) {
                if (skipCustomMsg) {
                    assert customDiscardId != null;

                    if (F.eq(customDiscardId, msg.id)) {
                        msg.msg = null;

                        return;
                    }
                }
            }
            else {
                if (skipMsg) {
                    assert discardId != null;

                    if (F.eq(discardId, msg.id)) {
                        msg.msg = null;

                        return;
                    }
                }
            }
        }
    }

    /**
     * Gets iterator for non-discarded messages.
     *
     * @return Non-discarded messages iterator.
     */
    public Iterator<TcpDiscoveryAbstractMessage> iterator() {
        return new SkipIterator();
    }

    /**
     *
     */
    private class SkipIterator implements Iterator<TcpDiscoveryAbstractMessage> {
        /** Skip non-custom messages flag. */
        private boolean skipMsg = discardId != null;

        /** Skip custom messages flag. */
        private boolean skipCustomMsg = customDiscardId != null;

        /** Internal iterator. */
        private Iterator<PendingMessage> msgIt = msgs.iterator();

        /** Next message. */
        private TcpDiscoveryAbstractMessage next;

        {
            advance();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return next != null;
        }

        /** {@inheritDoc} */
        @Override public TcpDiscoveryAbstractMessage next() {
            if (next == null)
                throw new NoSuchElementException();

            TcpDiscoveryAbstractMessage next0 = next;

            advance();

            return next0;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException();
        }

        /**
         * Advances iterator to the next available item.
         */
        private void advance() {
            next = null;

            while (msgIt.hasNext()) {
                PendingMessage msg0 = msgIt.next();

                if (msg0.customMsg) {
                    if (skipCustomMsg) {
                        assert customDiscardId != null;

                        if (F.eq(customDiscardId, msg0.id))
                            skipCustomMsg = false;

                        continue;
                    }
                }
                else {
                    if (skipMsg) {
                        assert discardId != null;

                        if (F.eq(discardId, msg0.id))
                            skipMsg = false;

                        continue;
                    }
                }

                if (msg0.msg == null)
                    continue;

                next = msg0.msg;

                break;
            }
        }
    }
}
