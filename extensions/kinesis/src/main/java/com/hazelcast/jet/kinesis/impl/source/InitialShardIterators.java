/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.kinesis.impl.source;

import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.AT_TIMESTAMP;

public class InitialShardIterators implements IdentifiedDataSerializable, Serializable {

    private List<Specification> specifications = new ArrayList<>();

    public GetShardIteratorRequest request(String stream, Shard shard) {
        for (Specification specification : specifications) {
            if (specification.matches(shard)) {
                return specification.request(stream, shard);
            }
        }
        return defaultRequest(stream, shard);
    }

    @Nonnull
    private GetShardIteratorRequest defaultRequest(String stream, Shard shard) {
        GetShardIteratorRequest request = new GetShardIteratorRequest();
        request.setStreamName(stream);
        request.setShardId(shard.getShardId());
        request.setShardIteratorType(AT_SEQUENCE_NUMBER);
        request.setStartingSequenceNumber(shard.getSequenceNumberRange().getStartingSequenceNumber());
        return request;
    }

    @Override
    public int getFactoryId() {
        return KinesisDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return KinesisDataSerializerHook.INITIAL_SHARD_ITERATORS;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(specifications.size());
        for (Specification specification : specifications) {
            out.writeObject(specification.pattern);
            out.writeObject(specification.shardIteratorType);
            out.writeObject(specification.parameter);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        specifications = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            specifications.add(new Specification(in.readObject(), in.readObject(), in.readObject()));
        }
    }

    public void add(
            @Nonnull String shardIdPattern,
            @Nonnull String shardIteratorType,
            @Nullable String parameter
    ) {
        specifications.add(new Specification(shardIdPattern, shardIteratorType, parameter));
    }

    private static final class Specification implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Pattern pattern;

        private final ShardIteratorType shardIteratorType;

        private final Object parameter;

        Specification(String shardIdRegExp, String shardIteratorType, String parameter) {
            try {
                this.pattern = Pattern.compile(Objects.requireNonNull(shardIdRegExp, "shardIdRegExp"));
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("'%s' doesn't seem to be a valid regular expression! " +
                        "Cause: %s", shardIdRegExp, e.getMessage()));
            }

            try {
                this.shardIteratorType = ShardIteratorType.valueOf(Objects.requireNonNull(shardIteratorType,
                        "shardIteratorType"));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("'%s' is not a valid shard iterator type. Acceptable " +
                        "values are: %s", shardIteratorType,
                        Arrays.stream(ShardIteratorType.values()).map(Enum::name).collect(Collectors.joining(", "))));
            }

            switch (this.shardIteratorType) {
                case AT_SEQUENCE_NUMBER:
                case AFTER_SEQUENCE_NUMBER:
                    this.parameter = parameter;
                    break;

                case AT_TIMESTAMP:
                    try {
                        this.parameter = new Date(Long.parseLong(parameter));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(String.format("For shard iterator type %s a timestamp string" +
                                " parsable as `long` must be provided", shardIteratorType));
                    }
                    break;

                case TRIM_HORIZON:
                case LATEST:
                    if (parameter != null) {
                        throw new IllegalArgumentException(String.format("For shard iterator type %s no parameter " +
                                "should be provided", shardIteratorType));
                    }
                    this.parameter = null;
                    break;

                default:
                    throw new IllegalArgumentException(String.format("Unknow shard iterator type: %s", shardIteratorType));
            }

        }

        public boolean matches(Shard shard) {
            String shardId = shard.getShardId();
            Matcher matcher = pattern.matcher(shardId);
            return matcher.matches();
        }

        public GetShardIteratorRequest request(String stream, Shard shard) {
            GetShardIteratorRequest request = new GetShardIteratorRequest();
            request.setStreamName(stream);
            request.setShardId(shard.getShardId());
            request.setShardIteratorType(shardIteratorType);
            if (shardIteratorType == AT_SEQUENCE_NUMBER || shardIteratorType == AFTER_SEQUENCE_NUMBER) {
                request.setStartingSequenceNumber((String) parameter);
            } else if (shardIteratorType == AT_TIMESTAMP) {
                request.setTimestamp((Date) parameter);
            }
            return request;
        }
    }
}
