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
package com.hazelcast.jet.mongodb.datalink.impl;

import com.hazelcast.datalink.DataLink;
import com.hazelcast.datalink.DataLinkRegistration;
import com.hazelcast.jet.mongodb.datalink.MongoDataLink;

/**
 * Registers {@link MongoDataLink}.
 *
 * @since 5.3
 */
public class MongoDataLinkRegistration implements DataLinkRegistration {

    /**
     * Returns "MongoDB" - will be used to determine Data link class based on it's this mapping type.
     */
    @Override
    public String type() {
        return "MongoDB";
    }

    /**
     * Returns class of {@link MongoDataLink}.
     */
    @Override
    public Class<? extends DataLink> clazz() {
        return MongoDataLink.class;
    }
}
