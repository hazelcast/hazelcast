/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableResolver;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for map-based table resolvers.
 */
public abstract class AbstractMapTableResolver implements TableResolver {

    protected final NodeEngine nodeEngine;
    protected final JetMapMetadataResolver jetMapMetadataResolver;
    private final List<List<String>> defaultSearchPaths;

    protected AbstractMapTableResolver(
        NodeEngine nodeEngine,
        JetMapMetadataResolver jetMapMetadataResolver,
        List<List<String>> defaultSearchPaths
    ) {
        this.nodeEngine = nodeEngine;
        this.jetMapMetadataResolver = jetMapMetadataResolver;
        this.defaultSearchPaths = defaultSearchPaths;
    }

    @Override
    public List<List<String>> getDefaultSearchPaths() {
        return defaultSearchPaths;
    }

    protected static List<TableField> mergeMapFields(Map<String, TableField> keyFields, Map<String, TableField> valueFields) {
        Map<String, TableField> res = new LinkedHashMap<>(keyFields);

        // Value fields do not override key fields.
        for (Map.Entry<String, TableField> valueFieldEntry : valueFields.entrySet()) {
            res.putIfAbsent(valueFieldEntry.getKey(), valueFieldEntry.getValue());
        }

        return new ArrayList<>(res.values());
    }
}
