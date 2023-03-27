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
import com.hazelcast.sql.impl.schema.datalink.DataLinkCatalogEntry;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.sql.impl.QueryUtils.wrapDataLinkKey;

public class DataLinkStorage extends AbstractSchemaStorage {

    public DataLinkStorage(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    public DataLinkCatalogEntry get(@Nonnull String name) {
        return (DataLinkCatalogEntry) storage().get(wrapDataLinkKey(name));
    }

    void put(@Nonnull String name, @Nonnull DataLinkCatalogEntry dataLinkCatalogEntry) {
        storage().put(wrapDataLinkKey(name), dataLinkCatalogEntry);
    }

    /**
     * @return true, if the datalink was added
     */
    boolean putIfAbsent(@Nonnull String name, @Nonnull DataLinkCatalogEntry dataLinkCatalogEntry) {
        return storage().putIfAbsent(wrapDataLinkKey(name), dataLinkCatalogEntry) == null;
    }

    /**
     * @return true, if the datalink was removed
     */
    boolean removeDataLink(@Nonnull String name) {
        return storage().remove(wrapDataLinkKey(name)) != null;
    }

    @Nonnull
    Collection<String> dataLinkNames() {
        return storage().values()
                .stream()
                .filter(m -> m instanceof DataLinkCatalogEntry)
                .map(m -> ((DataLinkCatalogEntry) m).name())
                .collect(Collectors.toList());
    }

    @Nonnull
    List<DataLinkCatalogEntry> dataLinks() {
        return storage().values()
                .stream()
                .filter(obj -> obj instanceof DataLinkCatalogEntry)
                .map(obj -> (DataLinkCatalogEntry) obj)
                .collect(Collectors.toList());
    }
}
