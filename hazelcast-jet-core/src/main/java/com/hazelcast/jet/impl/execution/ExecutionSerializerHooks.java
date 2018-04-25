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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

/**
 * Hazelcast serializer hooks for the classes in the {@code
 * com.hazelcast.jet.impl.execution} package. This is not a public-facing
 * API.
 */
class ExecutionSerializerHooks {
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
                public void destroy() {
                }

                @Override
                public void write(ObjectDataOutput out, SnapshotBarrier object) throws IOException {
                    out.writeLong(object.snapshotId());
                }

                @Override
                public SnapshotBarrier read(ObjectDataInput in) throws IOException {
                    return new SnapshotBarrier(in.readLong());
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
                public void destroy() {

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

    public static final class BroadcastKeyReferenceHook implements SerializerHook<BroadcastKeyReference> {

        @Override
        public Class<BroadcastKeyReference> getSerializationType() {
            return BroadcastKeyReference.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<BroadcastKeyReference>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.BROADCAST_KEY_REFERENCE;
                }

                @Override
                public void destroy() {

                }

                @Override
                public void write(ObjectDataOutput out, BroadcastKeyReference object) throws IOException {
                    out.writeObject(object.key());
                }

                @Override
                public BroadcastKeyReference read(ObjectDataInput in) throws IOException {
                    return new BroadcastKeyReference(in.readObject());
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return true;
        }
    }
}
