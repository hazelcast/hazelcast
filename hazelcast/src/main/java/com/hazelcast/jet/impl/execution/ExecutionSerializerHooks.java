/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;

/**
 * Hazelcast serializer hooks for the classes in the {@code
 * com.hazelcast.jet.impl.execution} package. This is not a public-facing
 * API.
 */
class ExecutionSerializerHooks {
    public static final class DoneItemHook implements SerializerHook<DoneItem> {

        @Override
        public Class<DoneItem> getSerializationType() {
            return DoneItem.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<DoneItem>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.DONE_ITEM;
                }

                @Override
                public void write(ObjectDataOutput out, DoneItem object) {
                }

                @Override
                public DoneItem read(ObjectDataInput in) {
                    return DoneItem.DONE_ITEM;
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class SnapshotBarrierHook implements SerializerHook<SnapshotBarrier> {

        @Override
        public Class<SnapshotBarrier> getSerializationType() {
            return SnapshotBarrier.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<SnapshotBarrier>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.SNAPSHOT_BARRIER;
                }

                @Override
                public void write(ObjectDataOutput out, SnapshotBarrier object) throws IOException {
                    out.writeLong(object.snapshotId());
                    out.writeBoolean(object.isTerminal());
                }

                @Override
                public SnapshotBarrier read(ObjectDataInput in) throws IOException {
                    return new SnapshotBarrier(in.readLong(), in.readBoolean());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class BroadcastEntryHook implements SerializerHook<BroadcastEntry> {

        @Override
        public Class<BroadcastEntry> getSerializationType() {
            return BroadcastEntry.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<BroadcastEntry>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.BROADCAST_ENTRY;
                }

                @Override
                public void write(ObjectDataOutput out, BroadcastEntry object) throws IOException {
                    out.writeObject(object.getKey());
                    out.writeObject(object.getValue());
                }

                @Override
                public BroadcastEntry read(ObjectDataInput in) throws IOException {
                    return new BroadcastEntry(in.readObject(), in.readObject());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }

    public static final class BroadcastKeyHook implements SerializerHook<BroadcastKey> {

        @Override
        public Class<BroadcastKey> getSerializationType() {
            return BroadcastKey.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<BroadcastKey>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.BROADCAST_KEY;
                }

                @Override
                public void write(ObjectDataOutput out, BroadcastKey object) throws IOException {
                    out.writeObject(object.key());
                }

                @Override
                public BroadcastKey read(ObjectDataInput in) throws IOException {
                    return broadcastKey(in.readObject());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }
}
