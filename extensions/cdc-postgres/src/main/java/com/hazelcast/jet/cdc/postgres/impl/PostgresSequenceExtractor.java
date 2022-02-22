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

package com.hazelcast.jet.cdc.postgres.impl;

import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.jet.cdc.impl.SequenceExtractor;

import java.util.Map;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

public class PostgresSequenceExtractor implements SequenceExtractor {

    private static final String SERVER = "server";
    private static final String LAST_WAL_SEQUENCE_NUMBER = "lsn";

    private String server;
    private long source;

    @Override
    public long sequence(Map<String, ?> debeziumOffset) {
        return (Long) debeziumOffset.get(LAST_WAL_SEQUENCE_NUMBER);
    }

    @Override
    public long source(Map<String, ?> debeziumPartition, Map<String, ?> debeziumOffset) {
        String server = (String) debeziumPartition.get(SERVER);
        if (isSourceNew(server)) {
            long source = computeSource(server);
            this.source = adjustForCollision(source);
            this.server = server;
        }
        return this.source;
    }

    private boolean isSourceNew(String server) {
        return !Objects.equals(this.server, server);
    }

    private static long computeSource(String server) {
        byte[] bytes = server.getBytes(UTF_8);
        return HashUtil.MurmurHash3_x64_64(bytes, 0, bytes.length);
    }

    private long adjustForCollision(long source) {
        if (this.source == source) {
            //source value should have changed, but hashing unfortunately
            //produced the same result; we need to adjust it
            if (source == Long.MAX_VALUE) {
                return Long.MIN_VALUE;
            } else {
                return Long.MAX_VALUE;
            }
        } else {
            return source;
        }
    }
}
