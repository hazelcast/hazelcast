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
import java.util.Map;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

public class PersonIdSerializer implements Serializer<PersonId> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, PersonId personId) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(outputStream)) {
            if (personId.getId() == null) {
                output.writeBoolean(false);
            } else {
                output.writeBoolean(true);
                output.writeInt(personId.getId());
            }
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
        return outputStream.toByteArray();
    }

    @Override
    public void close() {
    }
}
