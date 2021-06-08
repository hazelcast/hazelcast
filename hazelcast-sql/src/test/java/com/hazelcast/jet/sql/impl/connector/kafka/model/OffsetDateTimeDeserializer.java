/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.kafka.model;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

public class OffsetDateTimeDeserializer implements Deserializer<OffsetDateTime> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public OffsetDateTime deserialize(String topic, byte[] bytes) {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            int year = input.readShort();
            int month = input.readByte();
            int dayOfMonth = input.readByte();

            int hour = input.readByte();
            int minute = input.readByte();
            int second = input.readByte();
            int nano = input.readInt();

            int zoneTotalSeconds = input.readInt();
            ZoneOffset zoneOffset = ZoneOffset.ofTotalSeconds(zoneTotalSeconds);
            return OffsetDateTime.of(year, month, dayOfMonth, hour, minute, second, nano, zoneOffset);
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public void close() {
    }
}
