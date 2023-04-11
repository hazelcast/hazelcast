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

import org.bson.BsonDateTime;
import org.bson.BsonTimestamp;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;

import static java.time.ZoneId.systemDefault;
import static org.assertj.core.api.Assertions.assertThat;

public class MongoUtilitiesTest {

    @Test
    public void converts_bsonTimestamp_to_localDateTime() {
        //given
        long timeMillis = System.currentTimeMillis();
        LocalDateTime sourceDT = LocalDateTime.ofInstant(Instant.ofEpochMilli(timeMillis), systemDefault());

        // when
        BsonTimestamp bt = MongoUtilities.bsonTimestampFromTimeMillis(timeMillis);
        LocalDateTime dt = MongoUtilities.bsonTimestampToLocalDateTime(bt);

        // then
        // BsonTimestamp has second precision
        assertThat(dt).isEqualToIgnoringNanos(sourceDT);
    }

    @Test
    public void converts_bsonDateTime_to_localDateTime() {
        // given
        long timeMillis = System.currentTimeMillis();
        LocalDateTime sourceDT = LocalDateTime.ofInstant(Instant.ofEpochMilli(timeMillis), systemDefault());

        // when
        BsonDateTime bsonDateTime = new BsonDateTime(timeMillis);
        LocalDateTime dt = MongoUtilities.bsonDateTimeToLocalDateTime(bsonDateTime);

        // then
        assertThat(dt).isEqualTo(sourceDT);
    }

    @Test
    public void converts_localDateTime_to_bsonTimestamp() {
        // given
        long timeMillis = System.currentTimeMillis();
        LocalDateTime sourceDT = LocalDateTime.ofInstant(Instant.ofEpochMilli(timeMillis), systemDefault());

        // when
        BsonTimestamp ts = MongoUtilities.localDateTimeToTimestamp(sourceDT);
        LocalDateTime dt = MongoUtilities.bsonTimestampToLocalDateTime(ts);

        // then
        assertThat(dt).isEqualToIgnoringNanos(sourceDT);
    }
}
