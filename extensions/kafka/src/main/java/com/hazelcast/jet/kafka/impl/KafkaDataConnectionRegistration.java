/*
 * Copyright 2024 Hazelcast Inc.
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

package com.hazelcast.jet.kafka.impl;

import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.dataconnection.DataConnectionRegistration;
import com.hazelcast.jet.kafka.KafkaDataConnection;

/**
 * Registration for {@link KafkaDataConnection}
 */
public class KafkaDataConnectionRegistration implements DataConnectionRegistration {

    @Override
    public String type() {
        return "Kafka";
    }

    @Override
    public Class<? extends DataConnection> clazz() {
        return KafkaDataConnection.class;
    }
}
