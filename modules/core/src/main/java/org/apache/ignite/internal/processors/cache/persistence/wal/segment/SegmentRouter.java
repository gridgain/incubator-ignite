/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.segment;

import java.io.File;
import java.io.FileNotFoundException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;

import static org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor.fileName;

/**
 * Class for manage of segment location.
 */
public class SegmentRouter {
    private static final String ZIP_SUFFIX = ".zip";
    /** */
    private File walWorkDir;

    /** WAL archive directory (including consistent ID as subfolder) */
    private File walArchiveDir;

    /** Holder of actual information of latest manipulation on WAL segments. */
    private SegmentAware segmentAware;

    /** */
    private DataStorageConfiguration dsCfg;

    protected final FileIOFactory ioFactory;

    public SegmentRouter(File walWorkDir, File walArchiveDir,
        SegmentAware segmentAware, DataStorageConfiguration dsCfg,
        FileIOFactory ioFactory) {
        this.walWorkDir = walWorkDir;
        this.walArchiveDir = walArchiveDir;
        this.segmentAware = segmentAware;
        this.dsCfg = dsCfg;
        this.ioFactory = ioFactory;
    }

    /**
     * Find file which represent given segment.
     *
     * @param segmentId Segment for searching.
     * @return Actual file description.
     * @throws FileNotFoundException If file does not exist.
     */
    public FileDescriptor findSegment(long segmentId) throws FileNotFoundException {
        FileDescriptor fd;

        if (segmentAware.lastArchivedAbsoluteIndex() >= segmentId)
            fd = new FileDescriptor(new File(walArchiveDir, fileName(segmentId)));
        else
            fd = new FileDescriptor(new File(walWorkDir, fileName(segmentId % dsCfg.getWalSegments())), segmentId);

        if (!fd.file().exists()) {
            FileDescriptor zipFile = new FileDescriptor(new File(walArchiveDir, fileName(fd.idx()) + ZIP_SUFFIX));

            if (!zipFile.file().exists()) {
                throw new FileNotFoundException("Both compressed and raw segment files are missing in archive " +
                    "[segmentIdx=" + fd.idx() + "]");
            }

            fd = zipFile;
        }

        return fd;
    }
}
