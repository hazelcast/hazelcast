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

package com.hazelcast.sql.impl.schema.map.sample;

import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.LinkedHashMap;

/**
 * Metadata from sample resolution.
 */
public class MapSampleMetadata {

    private final QueryTargetDescriptor descriptor;
    private final LinkedHashMap<String, TableField> fields;

    public MapSampleMetadata(QueryTargetDescriptor descriptor, LinkedHashMap<String, TableField> fields) {
        this.descriptor = descriptor;
        this.fields = fields;
    }

    public QueryTargetDescriptor getDescriptor() {
        return descriptor;
    }

    @SuppressWarnings("checkstyle:IllegalType")
    public LinkedHashMap<String, TableField> getFields() {
        return fields;
    }
}
