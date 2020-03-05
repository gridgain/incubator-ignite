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
package org.apache.ignite.internal.pagemem.wal.record;

public class CRCLoggingRecord extends WALRecord {
    private final WALRecord record;
    private final long crc;
    private final long writtenCrc;
    private final long position;

    public CRCLoggingRecord(WALRecord record, long position, long crc, long writtenCrc) {
        this.record = record;
        this.crc = crc;
        this.writtenCrc = writtenCrc;
        this.position = position;
    }

    public RecordType type() {
        return this.record == null ? null : this.record.type();
    }

    public String toString() {
        return (this.record == null ? "null" : this.record.toString()) + " ; position=" + this.position + ", crc=" + this.crc + ", writtenCrc=" + this.writtenCrc;
    }
}
