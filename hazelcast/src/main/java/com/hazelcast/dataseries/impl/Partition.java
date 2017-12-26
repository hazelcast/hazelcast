/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
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

package com.hazelcast.dataseries.impl;

import com.hazelcast.config.DataSeriesConfig;
import com.hazelcast.dataseries.MemoryInfo;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class Partition {

    private final DataSeriesConfig config;
    private final long maxTenuringAgeNanos;

    // the segment receiving the writes.
    private Segment edenSegment;
    // the segment first in line to be evicted
    private Segment oldestTenuredSegment;
    private Segment youngestTenuredSegment;
    private int tenuredSegmentCount;
    private long sequence;

    public Partition(DataSeriesConfig config) {
        this.config = config;
        this.maxTenuringAgeNanos = config.getTenuringAgeMillis() == Long.MAX_VALUE
                ? Long.MAX_VALUE
                : MILLISECONDS.toNanos(config.getTenuringAgeMillis());

        //System.out.println(config);

        //System.out.println("record payload size:" + valueType.getPayloadSize());
    }

    private Segment newSegment() {

        return new Segment(config.getInitialSegmentSize(), config.getMaxSegmentSize());
    }

    public DataSeriesConfig getConfig() {
        return config;
    }

    private void ensureEdenExists() {
        boolean createEden = false;

        if (edenSegment == null) {
            // eden doesn't exist.
            createEden = true;
        } else if (maxTenuringAgeNanos != Long.MAX_VALUE
                && System.nanoTime() - edenSegment.firstInsertNanos() < maxTenuringAgeNanos) {
            // eden is expired
            createEden = true;
        }

        if (!createEden) {
            return;
        }

        //System.out.println("creating new eden segment");

        tenureEden();
        trim();
        edenSegment = newSegment();
    }

    private void tenureEden() {
        if (edenSegment == null) {
            return;
        }

        if (oldestTenuredSegment == null) {
            oldestTenuredSegment = edenSegment;
            youngestTenuredSegment = edenSegment;
        } else {
            edenSegment.previous = youngestTenuredSegment;
            youngestTenuredSegment.next = edenSegment;
            youngestTenuredSegment = edenSegment;
        }

        edenSegment = null;
        tenuredSegmentCount++;
    }

    // get rid of the oldest tenured segment if needed.
    private void trim() {
        int totalSegmentCount = tenuredSegmentCount + 1;

        if (totalSegmentCount > config.getSegmentsPerPartition()) {
            // we need to delete the oldest segment

            Segment victimSegment = oldestTenuredSegment;
            victimSegment.destroy();

            if (oldestTenuredSegment == youngestTenuredSegment) {
                oldestTenuredSegment = null;
                youngestTenuredSegment = null;
            } else {
                oldestTenuredSegment = victimSegment.next;
                oldestTenuredSegment.previous = null;
            }

            tenuredSegmentCount--;
        }
    }

    public void deleteRetiredSegments() {
        if (config.getTenuringAgeMillis() == Integer.MAX_VALUE) {
            // tenuring is disabled, so there will never be retired segments to delete
            return;
        }

        if (config.getSegmentsPerPartition() == Integer.MAX_VALUE) {
            // there is no limit on the number of segments per partition, so there is nothing to delete
            return;
        }

//        Segment segment = oldestTenuredSegment;
//        while (segment != null) {
//            Segment next = segment.next;
//
//
//
//            segment = next;
//            //todo: also deal with youngestTenuredSegment if last
//        }
    }

    public long append(byte[] value) {
        ensureEdenExists();

        //todo: it could be that not everything can be written
        return edenSegment.append(value);
    }

    public long count() {
        long count = 0;

        if (edenSegment != null) {
            count += edenSegment.count();
        }

        Segment segment = youngestTenuredSegment;
        while (segment != null) {
            count += segment.count();
            segment = segment.previous;
        }

        return count;
    }

    public MemoryInfo memoryInfo() {
        long consumedBytes = 0;
        long allocatedBytes = 0;
        int segmentsUsed = 0;

        if (edenSegment != null) {
            consumedBytes += edenSegment.consumedBytes();
            allocatedBytes += edenSegment.allocatedBytes();
            segmentsUsed++;
        }

        segmentsUsed += tenuredSegmentCount;

        Segment segment = youngestTenuredSegment;
        while (segment != null) {
            consumedBytes += segment.consumedBytes();
            allocatedBytes += segment.allocatedBytes();
            segment = segment.previous;
        }

        return new MemoryInfo(consumedBytes, allocatedBytes, segmentsUsed, count());
    }

    public Iterator iterator() {
        // we are not including eden for now. we rely on frozen partition
        return new IteratorImpl(oldestTenuredSegment);
    }

    class IteratorImpl implements Iterator {
        private Segment segment;
        private int recordIndex = -1;

        public IteratorImpl(Segment segment) {
            this.segment = segment;
        }

        @Override
        public boolean hasNext() {
            if (segment == null) {
                return false;
            }

            if (recordIndex == -1) {
                if (!segment.acquire()) {
                    segment = segment.next;
                    return hasNext();
                } else {
                    recordIndex = 0;
                }
            }

            if (recordIndex >= segment.count()) {
                segment.release();
                recordIndex = -1;
                segment = segment.next;
                return hasNext();
            }

            return true;
        }

        @Override
        public Object next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

//            Object o = encoder.newInstance();
//            encoder.read(o, segment.dataAddress(), recordIndex * valueType.getPayloadSize());
//            recordIndex++;
//            return o;

            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
