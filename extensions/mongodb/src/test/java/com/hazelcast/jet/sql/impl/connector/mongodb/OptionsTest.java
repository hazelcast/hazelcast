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
package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.bson.BsonTimestamp;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.Assert.assertThrows;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class OptionsTest {

    @Test
    public void parses_milis_startAt() {
        // given
        long time = System.currentTimeMillis();
        LocalDateTime timeDate = LocalDateTime.ofEpochSecond(time / 1000, 0, UTC);

        // when
        BsonTimestamp startAt = Options.startAtTimestamp(ImmutableMap.of(Options.START_AT_OPTION, String.valueOf(time)));

        // then
        LocalDateTime instant = LocalDateTime.ofEpochSecond(startAt.getTime(), 0, UTC);

        assertThat(instant).isEqualToIgnoringNanos(timeDate);
    }

    @Test
    public void parses_now_startAt() {
        // given
        long time = System.currentTimeMillis();
        LocalDateTime timeDate = LocalDateTime.ofEpochSecond(time / 1000, 0, UTC);

        // when
        BsonTimestamp startAt = Options.startAtTimestamp(ImmutableMap.of(Options.START_AT_OPTION, "now"));

        // then
        LocalDateTime instant = LocalDateTime.ofEpochSecond(startAt.getTime(), 0, UTC);

        assertThat(instant).isCloseTo(timeDate, within(1, ChronoUnit.SECONDS));
    }

    @Test
    public void parses_dateTimeString_startAt() {
        // given
        long time = System.currentTimeMillis();
        LocalDateTime timeDate = LocalDateTime.ofEpochSecond(time / 1000, 0, UTC);
        String dateAsString = timeDate.format(DateTimeFormatter.ISO_DATE_TIME) + "Z";

        // when
        BsonTimestamp startAt = Options.startAtTimestamp(ImmutableMap.of(Options.START_AT_OPTION, dateAsString));

        // then
        LocalDateTime instant = LocalDateTime.ofEpochSecond(startAt.getTime(), 0, UTC);
        assertThat(instant).isEqualToIgnoringNanos(timeDate);
    }

    @Test
    public void throws_at_invalid_dateTimeString() {
        // given
        long time = System.currentTimeMillis();
        LocalDateTime timeDate = LocalDateTime.ofEpochSecond(time / 1000, 0, UTC);
        String dateAsString = timeDate.format(DateTimeFormatter.ISO_DATE_TIME) + "BLABLABLA";

        // when
        QueryException queryException = assertThrows(QueryException.class, () ->
                Options.startAtTimestamp(ImmutableMap.of(Options.START_AT_OPTION, dateAsString)));

        // then
        assertThat(queryException).hasMessageContaining("Invalid startAt value:");
    }

}
