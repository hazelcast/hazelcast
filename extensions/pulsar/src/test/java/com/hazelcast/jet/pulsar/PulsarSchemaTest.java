/*
 * Copyright 2026 Hazelcast Inc.
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

package com.hazelcast.jet.pulsar;

import com.hazelcast.test.SerialTest;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@SerialTest
class PulsarSchemaTest extends PulsarTestSupport {

    @ParameterizedTest(name = "{0}")
    @MethodSource("schemas")
    @SuppressWarnings("unchecked")
    <T> void readsValueWrittenWithNativeSchema(String schemaName, Schema<T> nativeSchema,
                                               PulsarSchema<T> pulsarSchema, T value) throws Exception {
        String topic = randomName();
        createTopic(topic, false);

        Schema<T> readerSchema = (Schema<T>) pulsarSchema.supplierV4().get();
        assertThat(readerSchema.getSchemaInfo()).isEqualTo(nativeSchema.getSchemaInfo());
        try (var producer = getClient().newProducer(nativeSchema).topic(topic).create();
             var reader = getClient().newReader(readerSchema).topic(topic).startMessageId(MessageId.earliest).create()) {
            producer.send(value);

            var message = reader.readNext(10, SECONDS);
            assertThat(message).isNotNull();
            assertThat(message.getValue()).isEqualTo(value);
        }
    }

    static Stream<Arguments> schemas() {
        return Stream.of(
                arguments("BYTES", Schema.BYTES, PulsarSchema.bytes(), new byte[]{1, 2, 3}),
                arguments("BYTEBUFFER", Schema.BYTEBUFFER, PulsarSchema.byteBuffer(), ByteBuffer.wrap(new byte[]{1, 2, 3})),
                arguments("STRING", Schema.STRING, PulsarSchema.string(), "hello"),
                arguments("INT8", Schema.INT8, PulsarSchema.int8(), (byte) -8),
                arguments("INT16", Schema.INT16, PulsarSchema.int16(), (short) -16),
                arguments("INT32", Schema.INT32, PulsarSchema.int32(), -32),
                arguments("INT64", Schema.INT64, PulsarSchema.int64(), -64L),
                arguments("BOOL", Schema.BOOL, PulsarSchema.bool(), true),
                arguments("FLOAT", Schema.FLOAT, PulsarSchema.float32(), 1.25f),
                arguments("DOUBLE", Schema.DOUBLE, PulsarSchema.float64(), 2.5d),
                arguments("DATE", Schema.DATE, PulsarSchema.date(), new Date(1700000000123L)),
                arguments("TIME", Schema.TIME, PulsarSchema.time(), new Time(12345L)),
                arguments("TIMESTAMP", Schema.TIMESTAMP, PulsarSchema.timestamp(), new Timestamp(1700000000123L)),
                arguments("INSTANT", Schema.INSTANT, PulsarSchema.instant(), Instant.parse("2026-01-02T03:04:05.123456789Z")),
                arguments("LOCAL_DATE", Schema.LOCAL_DATE, PulsarSchema.localDate(), LocalDate.of(2026, 1, 2)),
                arguments("LOCAL_TIME", Schema.LOCAL_TIME, PulsarSchema.localTime(), LocalTime.of(3, 4, 5, 123456789)),
                arguments("LOCAL_DATE_TIME", Schema.LOCAL_DATE_TIME, PulsarSchema.localDateTime(),
                        LocalDateTime.of(2026, 1, 2, 3, 4, 5, 123456789))
        );
    }
}
