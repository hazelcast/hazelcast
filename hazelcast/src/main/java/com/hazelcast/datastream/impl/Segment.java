/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this segmentFile except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.datastream.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.config.DataStreamConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.datastream.impl.encoders.RecordEncoder;
import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.spi.serialization.SerializationService;
import sun.misc.Unsafe;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static java.lang.String.format;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

public class Segment {

    private final static AtomicIntegerFieldUpdater OWNERSHIP_COUNT = newUpdater(Segment.class, "ownershipCount");

    public volatile Segment next;
    public volatile Segment previous;

    // Provides 'smart pointer' like behavior so the segment can be shared between threads once it is tenured.
    // A segment is 'immutable' from that point on, but its resources need to be released once nobody is referring
    // to the segment. Using this mechanism we can safely share segments between threads.
    // we start with 1 because the partition is using the segment.
    private volatile int ownershipCount = 1;

    private final RecordEncoder encoder;
    private final SerializationService serializationService;
    private final Unsafe unsafe = UnsafeUtil.UNSAFE;
    private final RecordModel recordModel;
    private final long startNanos = System.nanoTime();
    private final long maxSegmentSize;
    private final long indicesAddress;
    private final Map<String, Aggregator> aggregators;
    private final long startOffset;
    private final File segmentFile;
    private final DataStreamConfig config;

    private long lastInsertNanos = startNanos;
    private long dataAddress;
    private long segmentSize;
    private long index = 0;

    Segment(String name,
            int partitionId,
            long startOffset,
            SerializationService serializationService,
            RecordModel recordModel,
            RecordEncoder encoder,
            Map<String, Aggregator> aggregators,
            DataStreamConfig config
    ) {
        this.config = config;
        this.segmentFile = new File(config.getStorageDir(),
                String.format("%02x%s-%08x-%016x.segment", name.length(), name, partitionId, startOffset));
        this.startOffset = startOffset;
        this.recordModel = recordModel;
        this.maxSegmentSize = config.getMaxSegmentSize();
        this.serializationService = serializationService;
        this.aggregators = aggregators;

        this.indicesAddress = recordModel.getIndexSize() == 0 ? 0 : unsafe.allocateMemory(recordModel.getIndexSize());
        if (indicesAddress != 0) {
            // set -1 on each bucket in the index
            for (int k = 0; k < recordModel.getIndexSize() / 4; k++) {
                unsafe.putInt(indicesAddress + k * 4, -1);
            }
        }
        this.dataAddress = unsafe.allocateMemory(config.getInitialSegmentSize());
        this.segmentSize = config.getInitialSegmentSize();
        this.encoder = encoder;
    }

    public long head() {
        return startOffset;
    }

    public long tail() {
        return startOffset + consumedBytes();
    }

    public boolean acquire() {
        for (; ; ) {
            int currentUsed = ownershipCount;
            if (currentUsed == 0) {
                // the segment has been destroyed.
                if (!loadFromFile()) {
                    return false;
                }
            }
            int newUsed = currentUsed + 1;
            if (OWNERSHIP_COUNT.compareAndSet(this, currentUsed, newUsed)) {
                return true;
            }
        }
    }

    public void release() {
        for (; ; ) {
            int currentUsed = ownershipCount;
            int newUsed = currentUsed - 1;
            if (OWNERSHIP_COUNT.compareAndSet(this, currentUsed, newUsed)) {
                if (newUsed == 0) {
                    saveToFile();
                    destroy0();
                }
                return;
            }
        }
    }

    public long count() {
        return index;
    }

    public long indicesAddress() {
        return indicesAddress;
    }

    public long dataAddress() {
        return dataAddress;
    }

    public Map<String, Aggregator> getAggregators() {
        return aggregators;
    }

    public long firstInsertNanos() {
        return startNanos;
    }

    public long lastInsertNanos() {
        return lastInsertNanos;
    }

    public long allocatedBytes() {
        return segmentSize;
    }

    public long consumedBytes() {
        // todo: the indices are not included
        return index * recordModel.getSize();
    }

    public void insert(Object valueData) {
        Object record = serializationService.toObject(valueData);
        if (record.getClass() != recordModel.getRecordClass()) {
            throw new RuntimeException(format("Expected value of class '%s', but found '%s' ",
                    record.getClass().getName(), recordModel.getRecordClass().getClass().getName()));
        }

        int recordOffset = (int) (index * recordModel.getSize());
        encoder.writeRecord(record, dataAddress, recordOffset, indicesAddress);
        for (Aggregator aggregator : aggregators.values()) {
            aggregator.accumulate(record);
        }
        index++;
        lastInsertNanos = System.nanoTime();
    }

    public boolean ensureCapacity() {
        long requiredSegmentSize = (index + 1) * recordModel.getSize();

        if (requiredSegmentSize < segmentSize) {
            return true;
        }

        if (maxSegmentSize <= requiredSegmentSize) {
            // we can't grow any further.
            return false;
        }

        long newSegmentSize = Math.min(maxSegmentSize, 2 * this.segmentSize);
        System.out.println("Growing segment from:" + this.segmentSize + " to:" + newSegmentSize);

        long newPointer = unsafe.allocateMemory(newSegmentSize);

        unsafe.copyMemory(dataAddress, newPointer, segmentSize);
        unsafe.freeMemory(dataAddress);

        this.segmentSize = newSegmentSize;
        this.dataAddress = newPointer;
        return true;
    }

    public void destroy() {
        release();
    }

    // does the actual destruction; will only be done where the last user ends using the segment
    private void destroy0() {
        //System.out.println("Destroying segment");

        unsafe.freeMemory(dataAddress);
        if (indicesAddress != 0) {
            unsafe.freeMemory(indicesAddress);
        }
    }

    /**
     * Loads the segment from file.
     *
     * @return true if loaded, false otherwise.
     */
    private boolean loadFromFile() {
        if(!config.isStorageEnabled()){
            return false;
        }

        long reportedFileSize = segmentFile.length();
        long newPointer = unsafe.allocateMemory(reportedFileSize);
        long totalRead = 0;
        try (InputStream in = new FileInputStream(segmentFile)) {
            byte[] buf = new byte[1 << 14];
            for (int readCount = 0; readCount != -1; readCount = in.read(buf)) {
                if (totalRead + readCount > reportedFileSize) {
                    throw new HazelcastException(String.format(
                            "%s: reported file size was %,3d, but more data was there, at least %,3d bytes",
                            segmentFile, reportedFileSize, totalRead + readCount));
                }
                MEM.copyFromByteArray(buf, 0, newPointer + totalRead, readCount);
                totalRead += readCount;
            }
            return true;
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException e) {
            throw new HazelcastException("Failed to load data from file " + segmentFile, e);
        }
    }

    private void saveToFile() {
        if(!config.isStorageEnabled()){
            return;
        }

        try (OutputStream out = new FileOutputStream(segmentFile)) {
            byte[] buf = new byte[1 << 14];
            long fileSize = index * recordModel.getSize();
            for (long offset = 0; offset < fileSize; offset += buf.length) {
                int batchSize = Math.min(buf.length, (int) (fileSize - offset));
                MEM.copyToByteArray(dataAddress + offset, buf, 0, batchSize);
                out.write(buf, 0, batchSize);
            }
        } catch (IOException e) {
            throw new HazelcastException("Failed to save data to file " + segmentFile, e);
        }
    }
}
