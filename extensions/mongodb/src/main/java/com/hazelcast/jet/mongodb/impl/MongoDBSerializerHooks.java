/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.mongodb.impl;

import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.TransactionOptions.Builder;
import com.mongodb.WriteConcern;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Hooks for serializing MongoDB non-serializable classes.
 */
@SuppressWarnings("unused") // used by com.hazelcast.SerializerHook file
public class MongoDBSerializerHooks {

    public static final class TransactionOptionsHook implements SerializerHook<TransactionOptions> {

        @Override
        public Class<TransactionOptions> getSerializationType() {
            return TransactionOptions.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<TransactionOptions>() {
                @Override
                public void write(@Nonnull ObjectDataOutput out, @Nonnull TransactionOptions object) throws IOException {
                    out.writeObject(object.getWriteConcern());
                    out.writeObject(object.getReadConcern());

                    ReadPreference readPreference = object.getReadPreference();
                    out.writeString(readPreference != null ? readPreference.getName() : null);

                    Long maxCommitTime = object.getMaxCommitTime(TimeUnit.MILLISECONDS);
                    out.writeObject(maxCommitTime);
                }

                @Nonnull
                @Override
                public TransactionOptions read(@Nonnull ObjectDataInput in) throws IOException {
                    WriteConcern writeConcern = in.readObject();
                    ReadConcern readConcern = in.readObject();
                    String readPreference = in.readString();
                    Long maxCommitTime = in.readObject();

                    Builder builder = TransactionOptions.builder();
                    if (writeConcern != null) {
                        builder.writeConcern(writeConcern);
                    }
                    if (readConcern != null) {
                        builder.readConcern(readConcern);
                    }
                    if (readPreference != null) {
                        builder.readPreference(ReadPreference.valueOf(readPreference));
                    }
                    if (maxCommitTime != null && maxCommitTime != -1) {
                        builder.maxCommitTime(maxCommitTime, TimeUnit.MILLISECONDS);
                    }
                    return builder.build();
                }

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.MONGO_TRANSACTION_OPTIONS;
                }
            };
        }



        @Override
        public boolean isOverwritable() {
            return false;
        }
    }

    public static final class ReadConcernHook implements SerializerHook<ReadConcern> {

        @Override
        public Class<ReadConcern> getSerializationType() {
            return ReadConcern.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<ReadConcern>() {
                @Override
                public void write(@Nonnull ObjectDataOutput out, @Nonnull ReadConcern object) throws IOException {
                    out.writeObject(object.getLevel());
                }

                @Nonnull
                @Override
                public ReadConcern read(@Nonnull ObjectDataInput in) throws IOException {
                    ReadConcernLevel level = in.readObject();
                    return level == null ? ReadConcern.DEFAULT : new ReadConcern(level);
                }

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.MONGO_READ_CONCERN;
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }
    }

    public static final class ReadConcernLevelHook implements SerializerHook<ReadConcernLevel> {

        @Override
        public Class<ReadConcernLevel> getSerializationType() {
            return ReadConcernLevel.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<ReadConcernLevel>() {
                @Override
                public void write(@Nonnull ObjectDataOutput out, @Nonnull ReadConcernLevel object) throws IOException {
                    out.writeString(object.getValue());
                }

                @Nonnull
                @Override
                public ReadConcernLevel read(@Nonnull ObjectDataInput in) throws IOException {
                   String level = in.readString();
                   requireNonNull(level, "ReadConcernLevel.value cannot be null");
                    return ReadConcernLevel.fromString(level);
                }

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.MONGO_READ_LEVEL;
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }
    }

    public static final class WriteConcernHook implements SerializerHook<WriteConcern> {

        @Override
        public Class<WriteConcern> getSerializationType() {
            return WriteConcern.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<WriteConcern>() {
                @Override
                public void write(@Nonnull ObjectDataOutput out, @Nonnull WriteConcern writeConcern) throws IOException {
                    out.writeObject(writeConcern.getWObject());
                    out.writeObject(writeConcern.getWTimeout(TimeUnit.MILLISECONDS));
                    out.writeObject(writeConcern.getJournal());
                }

                @Nonnull
                @Override
                public WriteConcern read(@Nonnull ObjectDataInput in) throws IOException {
                    Object w = in.readObject();
                    Object timeout = in.readObject();
                    Object journal = in.readObject();
                    if (w == null && timeout == null && journal == null) {
                        return WriteConcern.ACKNOWLEDGED;
                    }
                    requireNonNull(w, "WriteConcern.w cannot be null");
                    WriteConcern writeConcern;
                    if (w instanceof String) {
                        writeConcern = new WriteConcern((String) w);
                    } else {
                        requireNonNull(timeout, "WriteConcern.timeout cannot be null");
                        writeConcern = new WriteConcern((Integer) w, (Integer) timeout);
                    }
                    writeConcern.withJournal((Boolean) journal);
                    return writeConcern;
                }

                @Override
                public int getTypeId() {
                    return SerializerHookConstants.MONGO_WRITE_CONCERN;
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }
    }

}
