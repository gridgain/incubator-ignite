package org.apache.ignite.internal.util.nio.operation;

/**
 * Asynchronous operation that may be requested on selector.
 */
public enum NioOperation {
    /** Register connect key selection. */
    CONNECT,

    /** Cancel connect. */
    CANCEL_CONNECT,

    /** Register read key selection. */
    REGISTER,

    /** Move session between workers. */
    START_MOVE,

    /** Move session between workers. */
    FINISH_MOVE,

    /** Register write key selection. */
    REQUIRE_WRITE,

    /** Close key. */
    CLOSE,

    /** Pause read. */
    PAUSE_READ,

    /** Resume read. */
    RESUME_READ,

    /** Dump statistics. */
    DUMP_STATS,

    /** */
    RECALCULATE_IDLE_TIMEOUT
}
