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
import com.hazelcast.sql.impl.schema.dataconnection.DataConnectionCatalogEntry;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.sql.impl.QueryUtils.wrapDataConnectionKey;

public class DataConnectionStorage extends AbstractSchemaStorage {

    public DataConnectionStorage(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    public DataConnectionCatalogEntry get(@Nonnull String name) {
        return (DataConnectionCatalogEntry) storage().get(wrapDataConnectionKey(name));
    }

    void put(@Nonnull String name, @Nonnull DataConnectionCatalogEntry dataConnectionCatalogEntry) {
        storage().put(wrapDataConnectionKey(name), dataConnectionCatalogEntry);
    }

    /**
     * @return true, if the data connection was added
     */
    boolean putIfAbsent(@Nonnull String name, @Nonnull DataConnectionCatalogEntry dataConnectionCatalogEntry) {
        return storage().putIfAbsent(wrapDataConnectionKey(name), dataConnectionCatalogEntry) == null;
    }

    /**
     * @return true, if the data connection was removed
     */
    boolean removeDataConnection(@Nonnull String name) {
        return storage().remove(wrapDataConnectionKey(name)) != null;
    }

    @Nonnull
    Collection<String> dataConnectionNames() {
        return storage().values()
                .stream()
                .filter(m -> m instanceof DataConnectionCatalogEntry)
                .map(m -> ((DataConnectionCatalogEntry) m).name())
                .collect(Collectors.toList());
    }

    @Nonnull
    List<DataConnectionCatalogEntry> dataConnections() {
        return storage().values()
                .stream()
                .filter(obj -> obj instanceof DataConnectionCatalogEntry)
                .map(obj -> (DataConnectionCatalogEntry) obj)
                .collect(Collectors.toList());
    }
}
