package org.apache.ignite.internal.processors.odbc;

import java.util.concurrent.atomic.LongAdder;

/**
 * Client listener metrics.
 */
public class ClientListenerMetrics {
    /** Intervals. */
    private static final int INTERVALS = 50;

    /** Holder. */
    private volatile Holder holder = new Holder();

    /**
     * Handle client connected.
     */
    public void onConnected() {
        holder.onConnected();
    }

    /**
     * Handle query executed.
     *
     * @param dur Duration.
     */
    public void onQueryExecuted(long dur) {
        holder.onQueryExecuted(dur);
    }

    /**
     * Reset counters.
     */
    public void reset() {
        holder = new Holder();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return holder.toString();
    }

    /**
     * Holder object.
     */
    private final class Holder {
        /** Connection counter. */
        private final LongAdder connCtr = new LongAdder();

        /** Query counter. */
        private final LongAdder qryCtr = new LongAdder();

        /** Duration counters. */
        private final LongAdder[] durCtrs;

        /** Counter for long requests. */
        private final LongAdder durLongCtr = new LongAdder();

        /**
         * Constructor.
         */
        private Holder() {
            LongAdder[] durCtrs0 = new LongAdder[INTERVALS];

            for (int i = 0; i < INTERVALS; i++)
                durCtrs0[i] = new LongAdder();

            durCtrs = durCtrs0;
        }

        /**
         * Handle client connected.
         */
        private void onConnected() {
            connCtr.increment();
        }

        /**
         * Handle query executed.
         *
         * @param dur Duration.
         */
        private void onQueryExecuted(long dur) {
            if (dur < 0) {
                durCtrs[0].increment();

                return;
            }

            int dur0 = (int)dur;

            if ((long)dur0 == dur) {
                int slot = dur0 / INTERVALS;

                if (slot < durCtrs.length) {
                    durCtrs[slot].increment();

                    return;
                }
            }

            durLongCtr.increment();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
        @Override public String toString() {
            StringBuilder sb = new StringBuilder("[conn=" + connCtr.longValue() + ", ");

            for (int i = 0; i < INTERVALS; i++) {
                long durVal = durCtrs[i].longValue();

                if (durVal != 0)
                    sb.append("" + (i * 100) + "=" + durVal + ", ");
            }

            long durLongVal = durLongCtr.longValue();

            if (durLongVal != 0)
                sb.append("" + (INTERVALS * 100) + "+=" + durLongVal);

            sb.append("]");

            return sb.toString();
        }
    }
}
