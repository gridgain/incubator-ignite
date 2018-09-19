package org.apache.ignite.internal.jdbc.thin;

import java.io.IOException;
import java.io.Writer;
import java.util.concurrent.atomic.LongAdder;

/**
 * JDBC query metrics.
 */
public class JdbcMetrics {
    /** Intervals. */
    private static final int INTERVALS = 100;

    /** Interval size (in milliseconds). */
    private static final int INTERVAL_SIZE = 50;

    /** Total metrics. */
    private volatile Holder totalHolder = new Holder();

    /** Metrics since last printout. */
    private volatile Holder holder = new Holder();

    /**
     * Handle query executed.
     *
     * @param dur Duration.
     */
    public void onQueryExecuted(long dur) {
        totalHolder.onQueryExecuted(dur);
        holder.onQueryExecuted(dur);
    }

    /**
     * Print metrics and reset them.
     *
     * @param writer Writer to write data to.
     */
    public void printAndReset(Writer writer) {
        Holder deltaHolder = holder;

        holder = new Holder();

        String total = ">>> JDBC TOTAL: " + totalHolder.toString();
        String delta = ">>> JDBC DELTA: " + deltaHolder.toString();

        try {
            writer.write(total + "\n");
            writer.write(delta + "\n");
            writer.write("\n");

            writer.flush();
        }
        catch (IOException e) {
            System.err.println(">>> JDBC metrics writer failed: " + e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return holder.toString();
    }

    /**
     * Holder object.
     */
    private final class Holder {
        /** Query counter. */
        private final LongAdder qryCtr = new LongAdder();

        /** Duration counters. */
        private final LongAdder[] durCtrs;

        /** Counter for long requests. */
        private final LongAdder durLongCtr = new LongAdder();

        /** Total duration. */
        private final LongAdder totalDur = new LongAdder();

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
         * Handle query executed.
         *
         * @param dur Duration.
         */
        private void onQueryExecuted(long dur) {
            qryCtr.increment();

            totalDur.add(dur);

            if (dur < 0) {
                durCtrs[0].increment();

                return;
            }

            int dur0 = (int)dur;

            if ((long)dur0 == dur) {
                int slot = dur0 / INTERVAL_SIZE;

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
            long qryCtrVal = qryCtr.longValue();

            StringBuilder sb = new StringBuilder("[queries=" + qryCtrVal);

            double meanLat;

            if (qryCtrVal != 0)
                meanLat = (double)totalDur.longValue() / qryCtr.longValue();
            else
                meanLat = 0;

            sb.append(", " + "meanLat=" + meanLat);

            for (int i = 0; i < INTERVALS; i++) {
                long durVal = durCtrs[i].longValue();

                if (durVal != 0)
                    sb.append(", " + ((i + 1) * INTERVAL_SIZE) + "=" + durVal);
            }

            long durLongVal = durLongCtr.longValue();

            if (durLongVal != 0)
                sb.append(", " + (INTERVALS * INTERVAL_SIZE) + "+=" + durLongVal);

            sb.append("]");

            return sb.toString();
        }
    }
}
