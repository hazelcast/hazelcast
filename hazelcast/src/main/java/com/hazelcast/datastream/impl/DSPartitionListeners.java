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

    DSPartitionListeners(DSService service, String name, DSPartition partition) {
        this.name = name;
        this.service = service;
        this.partition = partition;
    }

    public void registerRemoteListener(String uuid, Connection connection, long offset) {
        remoteListeners.add(new RemoteListener(uuid, connection, offset));
    }

    public void registerLocalListener(Consumer consumer, long offset) {
        localListeners.add(new LocalListener(offset, consumer));
    }

    // called from the partition thread
    public void onAppend(DSPartition partition) {
        for (RemoteListener subscription : remoteListeners) {

        }

        for(LocalListener localListener:localListeners){
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

    private class LocalListener{
        private final Segment segment;
        private long offset;
        private Consumer<Data> consumer;

        public LocalListener(long offset, Consumer<Data> consumer) {
            this.offset = offset;
            this.consumer = consumer;
            this.segment = partition.findSegment(offset);
        }

        public void onAppend() {

        }
    }

//    class IteratorImpl implements Iterator {
//        private Segment segment;
//        private int recordIndex = -1;
//
//        public IteratorImpl(Segment segment) {
//            this.segment = segment;
//        }
//
//        @Override
//        public boolean hasNext() {
//            if (segment == null) {
//                return false;
//            }
//
//            if (recordIndex == -1) {
//                if (!segment.acquire()) {
//                    segment = segment.next;
//                    return hasNext();
//                } else {
//                    recordIndex = 0;
//                }
//            }
//
//            if (recordIndex >= segment.count()) {
//                segment.release();
//                recordIndex = -1;
//                segment = segment.next;
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
//            encoder.readRecord(o, segment.dataAddress(), recordIndex * recordModel.getPayloadSize());
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
