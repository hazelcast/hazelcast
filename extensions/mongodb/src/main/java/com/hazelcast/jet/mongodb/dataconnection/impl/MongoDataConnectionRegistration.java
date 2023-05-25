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
package com.hazelcast.jet.mongodb.dataconnection.impl;

import com.hazelcast.dataconnection.DataConnection;
import com.hazelcast.dataconnection.DataConnectionRegistration;
import com.hazelcast.jet.mongodb.dataconnection.MongoDataConnection;

/**
 * Registers {@link MongoDataConnection}.
 *
 * @since 5.3
 */
public class MongoDataConnectionRegistration implements DataConnectionRegistration {

    /**
     * Returns "Mongo" - will be used to determine DataConnection class based on it's this mapping type.
     */
    @Override
    public String type() {
        return "Mongo";
    }

    /**
     * Returns class of {@link MongoDataConnection}.
     */
    @Override
    public Class<? extends DataConnection> clazz() {
        return MongoDataConnection.class;
    }
}
