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

package com.hazelcast.jet.sql.impl.connector.kafka.model;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

public class OffsetDateTimeSerializer implements Serializer<OffsetDateTime> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, OffsetDateTime timestamp) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(outputStream)) {
            int year = timestamp.getYear();
            int monthValue = timestamp.getMonthValue();
            int dayOfMonth = timestamp.getDayOfMonth();
            output.writeShort(year);
            output.writeByte(monthValue);
            output.writeByte(dayOfMonth);

            int hour = timestamp.getHour();
            int minute = timestamp.getMinute();
            int second = timestamp.getSecond();
            int nano = timestamp.getNano();
            output.writeByte(hour);
            output.writeByte(minute);
            output.writeByte(second);
            output.writeInt(nano);

            ZoneOffset offset = timestamp.getOffset();
            int totalSeconds = offset.getTotalSeconds();
            output.writeInt(totalSeconds);
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
        return outputStream.toByteArray();
    }

    @Override
    public void close() {
    }
}
