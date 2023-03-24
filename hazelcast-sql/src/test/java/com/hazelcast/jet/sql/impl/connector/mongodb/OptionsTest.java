package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.TemporalOffset;
import org.bson.BsonTimestamp;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class OptionsTest {

    @Test
    public void parses_milis_startAt() {
        // given
        long time = System.currentTimeMillis();
        LocalDateTime timeDate = LocalDateTime.ofEpochSecond(time / 1000, 0, ZoneOffset.UTC);

        // when
        BsonTimestamp startAt = Options.startAt(ImmutableMap.of(Options.START_AT_OPTION, String.valueOf(time)));

        // then
        LocalDateTime instant = LocalDateTime.ofEpochSecond(startAt.getTime(), 0, ZoneOffset.UTC);

        assertThat(instant).isEqualToIgnoringNanos(timeDate);
    }

    @Test
    public void parses_now_startAt() {
        // given
        long time = System.currentTimeMillis();
        LocalDateTime timeDate = LocalDateTime.ofEpochSecond(time / 1000, 0, ZoneOffset.UTC);

        // when
        BsonTimestamp startAt = Options.startAt(ImmutableMap.of(Options.START_AT_OPTION, "now"));

        // then
        LocalDateTime instant = LocalDateTime.ofEpochSecond(startAt.getTime(), 0, ZoneOffset.UTC);

        assertThat(instant).isCloseTo(timeDate, within(1, ChronoUnit.SECONDS));
    }

    @Test
    public void parses_dateTimeString_startAt() {
        // given
        long time = System.currentTimeMillis();
        LocalDateTime timeDate = LocalDateTime.ofEpochSecond(time / 1000, 0, ZoneOffset.UTC);
        String dateAsString = timeDate.format(DateTimeFormatter.ISO_DATE_TIME) + "Z";

        // when
        BsonTimestamp startAt = Options.startAt(ImmutableMap.of(Options.START_AT_OPTION, dateAsString));

        // then
        LocalDateTime instant = LocalDateTime.ofEpochSecond(startAt.getTime(), 0, ZoneOffset.UTC);
        assertThat(instant).isEqualToIgnoringNanos(timeDate);
    }

}