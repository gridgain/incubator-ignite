/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.IOException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;

/**
 * Factory for {@link FileInput}.
 */
public interface FileInputFactory {
    /**
     * @param segmentId Segment represented by fileIO.
     * @param fileIO FileIO of segment for reading.
     * @param buf ByteBuffer wrapper for dynamically expand buffer size.
     * @return Instance of {@link FileInput}.
     * @throws IOException
     */
    FileInput createFileInput(long segmentId, FileIO fileIO, ByteBufferExpander buf) throws IOException;
}
