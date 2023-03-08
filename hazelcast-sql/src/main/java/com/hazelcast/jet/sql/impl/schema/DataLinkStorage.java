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
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.sql.impl.QueryUtils.wrapDataLinkKey;

public class DataLinkStorage extends AbstractSchemaStorage {

    public DataLinkStorage(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    void put(String name, DataLink dataLink) {
        storage().put(wrapDataLinkKey(name), dataLink);
    }

    boolean putIfAbsent(String name, DataLink dataLink) {
        return storage().putIfAbsent(wrapDataLinkKey(name), dataLink) == null;
    }

    DataLink removeDataLink(String name) {
        return (DataLink) storage().remove(wrapDataLinkKey(name));
    }

    Collection<String> dataLinkNames() {
        return storage().values()
                .stream()
                .filter(m -> m instanceof DataLink)
                .map(m -> ((DataLink) m).name())
                .collect(Collectors.toList());
    }

    List<DataLink> dataLinks() {
        return storage().values()
                .stream()
                .filter(obj -> obj instanceof DataLink)
                .map(obj -> (DataLink) obj)
                .collect(Collectors.toList());
    }
}
