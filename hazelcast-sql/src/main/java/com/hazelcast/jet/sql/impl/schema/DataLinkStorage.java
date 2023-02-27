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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.schema.datalink.DataLink;

import java.util.Collection;
import java.util.stream.Collectors;

public class DataLinkStorage extends AbstractSchemaStorage {

    DataLinkStorage(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    void put(String name, DataLink dataLink) {
        storage().put(name, dataLink);
    }

    boolean putIfAbsent(String name, DataLink dataLink) {
        return storage().putIfAbsent(name, dataLink) == null;
    }

    DataLink removeDataLink(String name) {
        return (DataLink) storage().remove(name);
    }

    Collection<String> dataLinkNames() {
        return storage().values()
                .stream()
                .filter(m -> m instanceof DataLink)
                .map(m -> ((DataLink) m).getName())
                .collect(Collectors.toList());
    }
}
