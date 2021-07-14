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

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

public class PersonDeserializer implements Deserializer<Person> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Person deserialize(String topic, byte[] bytes) {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            Person person = new Person();
            if (input.readBoolean()) {
                person.setId(input.readInt());
            }
            if (input.readBoolean()) {
                person.setName(input.readUTF());
            }
            return person;
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    @Override
    public void close() {
    }
}
