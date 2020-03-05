/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Date;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.InitNewPageRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.logger.NullLogger;

public class Main {
    public static void main(String[] args) throws IgniteCheckedException {
        String walPath = args[0];



        File[] walFiles = !F.isEmpty(walPath) ? files(walPath, null, null) : null;
        File[] walArchiveFiles = null;

        System.setProperty(IgniteSystemProperties.IGNITE_TO_STRING_MAX_LENGTH, String.valueOf(Integer.MAX_VALUE));

        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(new NullLogger());

        if (walFiles != null) {
            IgniteWalIteratorFactory.IteratorParametersBuilder builder = new IgniteWalIteratorFactory.IteratorParametersBuilder().
                pageSize(4096).
                binaryMetadataFileStoreDir(null).
                marshallerMappingFileStoreDir(null).
                keepBinary(false).
                filesOrDirs(walFiles);

            if (walArchiveFiles != null)
                builder.filesOrDirs(walArchiveFiles);

            try (WALIterator it = factory.iterator(builder)) {
                while (it.hasNextX()) {
                    IgniteBiTuple<WALPointer, WALRecord> t = it.nextX();
                    printRecord(t.get1(), t.get2(), null, null, false, -1);
                }
            }

            System.out.println("End of WAL file.");
        }
    }


    /**
     * @param path WAL directory path.
     * @param fromTime The start time of the last modification of the files.
     * @param toTime The end time of the last modification of the files.
     * @return WAL files.
     */
    private static File[] files(String path, Long fromTime, Long toTime) {
        final File dir = new File(path);

        if (!dir.exists() || !dir.isDirectory())
            throw new IllegalArgumentException("Incorrect directory path: " + path);

        final File[] files = dir.listFiles(new FileFilter() {
            @Override public boolean accept(File f) {
                if (!FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER.accept(f))
                    return false;

                long fileLastModifiedTime = getFileLastModifiedTime(f);

                if (fromTime != null && fileLastModifiedTime < fromTime)
                    return false;

                if (toTime != null && fileLastModifiedTime > toTime)
                    return false;

                return true;
            }
        });

        if (files != null) {
            Arrays.sort(files);

            for (File file : files) {
                try {
                    System.out.printf("'%s': %s%n", file, new Date(getFileLastModifiedTime(file)));
                }
                catch (Exception e) {
                    System.err.printf("Failed to get last modified time of file '%s'%n", file);
                }
            }
        }
        else
            System.out.printf("'%s' is empty%n", path);

        return files;
    }

    /**
     * @param f WAL file.
     * @return The last modification time of file.
     */
    private static long getFileLastModifiedTime(File f) {
        try {
            return Files.getLastModifiedTime(f.toPath()).toMillis();
        }
        catch (IOException e) {
            System.err.printf("Failed to get file '%s' modification time%n", f);

            return -1;
        }
    }

    /**
     * @param pos pos.
     * @param record WAL record.
     * @param types WAL record types.
     * @param recordContainsText Filter by substring in the WAL record.
     * @param archive Archive flag.
     * @param pageId pageId.
     */
    private static void printRecord(WALPointer pos, WALRecord record, Set<String> types, String recordContainsText,
        boolean archive, long pageId) {
        if (F.isEmpty(types) || types.contains(record.type().name())) {
            try {
                if (pageId != -1) {
                    if (record instanceof InitNewPageRecord) {
                        InitNewPageRecord initRec = (InitNewPageRecord) record;

                        if ((initRec.pageId() == pageId || initRec.newPageId() == pageId)) {
                            System.out.println("InitNewPageRecord:: pos=" + pos
                                + ", rec=" + initRec.toString() + ", ioType=" + initRec.ioType()
                                + ", ioVersion=" + initRec.ioVersion());

                            assert (PageIdUtils.partId(initRec.newPageId()) ==
                                PageIdUtils.partId(initRec.pageId()));
                        }
                    }
                }
                else {
                    String recordStr = record.toString();

                    if (F.isEmpty(recordContainsText) || recordStr.contains(recordContainsText))
                        System.out.printf("[%s] %s%n", archive ? "A" : "W", recordStr);
                }
            }
            catch (Exception e) {
                System.err.printf("Failed to print record (type=%s)%n", record.type());
            }
        }
    }
}
