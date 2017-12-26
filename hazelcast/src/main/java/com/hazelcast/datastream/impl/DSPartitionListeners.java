/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl;

import com.hazelcast.internal.commitlog.Region;
import com.hazelcast.internal.commitlog.Encoder;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.function.Consumer;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class DSPartitionListeners {

    private final String name;
    private final DSService service;
    private final DSPartition partition;
    private final ArrayList<RemoteListener> remoteListeners = new ArrayList<RemoteListener>();
    private final ArrayList<LocalListener> localListeners = new ArrayList<LocalListener>();
    private final InternalSerializationService ss;

    DSPartitionListeners(DSService service, String name, DSPartition partition, InternalSerializationService ss) {
        this.name = name;
        this.service = service;
        this.partition = partition;
        this.ss = ss;
    }

    public void registerRemoteListener(String uuid, Connection connection, long offset) {
        remoteListeners.add(new RemoteListener(uuid, connection, offset));
    }

    public void registerLocalListener(Consumer consumer, long offset) {
        LocalListener l = new LocalListener(offset, consumer);
        localListeners.add(l);
        // this will trigger sending whatever is available.
        l.onAppend();
    }

    // called from the partition thread
    public void onAppend(DSPartition partition) {
        for (RemoteListener subscription : remoteListeners) {

        }

        for (LocalListener localListener : localListeners) {
            localListener.onAppend();
        }
    }

    private class RemoteListener {
        private final String uuid;
        private long offset;
        private Connection connection;
        private final AtomicLong bytesInFlight;

        RemoteListener(String uuid, Connection connection, long offset) {
            this.bytesInFlight = service.getBytesInFlight(uuid);
            this.uuid = uuid;
            this.connection = connection;
            this.offset = offset;
        }
    }

    private class LocalListener {
        private final Encoder encoder;
        // todo: the issue is that the region is now hogged.
        private Region region;
        private long offset;
        private Consumer<Data> consumer;

        LocalListener(long offset, Consumer<Data> consumer) {
            this.offset = offset;
            this.consumer = consumer;
            this.encoder = partition.encoder();
        }

        // called
        //todo: we need to acquire the region.
        void onAppend() {
            // System.out.println("on Append called: region "+region);
            if (region == null) {
                this.region = partition.findRegion(offset);
                if (region == null) {
                    return;
                }
            }

            for (; ; ) {
                int dataOffset = (int) (this.offset - region.head());
                if (dataOffset >= region.dataOffset()) {
                    region = region.next;
                    if (region == null) {
                        return;
                    }
                    offset = region.head();
                    continue;
                }
                encoder.dataOffset = dataOffset;
                encoder.dataAddress = region.dataAddress();
                HeapData load = encoder.load();
                System.out.println("loaded:" + ss.toObject(load));
                consumer.accept(load);
                this.offset = region.head() + encoder.dataOffset;
            }
        }
    }

//    class IteratorImpl implements Iterator {
//        private Segment region;
//        private int recordIndex = -1;
//
//        public IteratorImpl(Segment region) {
//            this.region = region;
//        }
//
//        @Override
//        public boolean hasNext() {
//            if (region == null) {
//                return false;
//            }
//
//            if (recordIndex == -1) {
//                if (!region.acquire()) {
//                    region = region.next;
//                    return hasNext();
//                } else {
//                    recordIndex = 0;
//                }
//            }
//
//            if (recordIndex >= region.count()) {
//                region.release();
//                recordIndex = -1;
//                region = region.next;
//                return hasNext();
//            }
//
//            return true;
//        }
//
//        @Override
//        public Object next() {
//            if (!hasNext()) {
//                throw new NoSuchElementException();
//            }
//
//            Object o = encoder.newInstance();
//            encoder.readRecord(o, region.dataAddress(), recordIndex * recordModel.getPayloadSize());
//            recordIndex++;
//            return o;
//        }
//
//        @Override
//        public void remove() {
//            throw new UnsupportedOperationException();
//        }
//    }
}
