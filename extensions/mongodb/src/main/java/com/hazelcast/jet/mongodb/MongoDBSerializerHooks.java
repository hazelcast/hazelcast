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
package com.hazelcast.jet.mongodb;

import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.TransactionOptions.Builder;
import com.mongodb.WriteConcern;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

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
                    WriteConcern writeConcern = object.getWriteConcern();
                    ReadConcern readConcern = object.getReadConcern();
                    ReadPreference readPreference = object.getReadPreference();
                    Long maxCommitTime = object.getMaxCommitTime(TimeUnit.MILLISECONDS);
                    out.writeObject(writeConcern);
                    out.writeObject(readConcern);
                    out.writeString(readPreference != null ? readPreference.getName() : null);
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
            return true;
        }
    }

}
