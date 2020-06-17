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

package com.hazelcast.sql.impl.schema.map.options;

import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.inject.UpsertTargetDescriptor;

import java.util.LinkedHashMap;

/**
 * Metadata from options resolution.
 */
// TODO: deduplicate with MapSampleMetadata ?
public class MapOptionsMetadata {

    private final QueryTargetDescriptor queryTargetDescriptor;
    private final UpsertTargetDescriptor upsertTargetDescriptor;
    private final LinkedHashMap<String, QueryPath> fields;

    public MapOptionsMetadata(QueryTargetDescriptor queryTargetDescriptor,
                              UpsertTargetDescriptor upsertTargetDescriptor,
                              LinkedHashMap<String, QueryPath> fields) {
        this.queryTargetDescriptor = queryTargetDescriptor;
        this.upsertTargetDescriptor = upsertTargetDescriptor;
        this.fields = fields;
    }

    public QueryTargetDescriptor getQueryTargetDescriptor() {
        return queryTargetDescriptor;
    }

    public UpsertTargetDescriptor getUpsertTargetDescriptor() {
        return upsertTargetDescriptor;
    }

    @SuppressWarnings("checkstyle:IllegalType")
    public LinkedHashMap<String, QueryPath> getFields() {
        return fields;
    }
}
