/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.List;

public class KvMetadata {

    private final List<TableField> fields;
    private final QueryTargetDescriptor queryTargetDescriptor;
    private final UpsertTargetDescriptor upsertTargetDescriptor;

    public KvMetadata(
            List<TableField> fields,
            QueryTargetDescriptor queryTargetDescriptor,
            UpsertTargetDescriptor upsertTargetDescriptor
    ) {
        this.fields = fields;
        this.queryTargetDescriptor = queryTargetDescriptor;
        this.upsertTargetDescriptor = upsertTargetDescriptor;
    }

    public List<TableField> getFields() {
        return fields;
    }

    public QueryTargetDescriptor getQueryTargetDescriptor() {
        return queryTargetDescriptor;
    }

    public UpsertTargetDescriptor getUpsertTargetDescriptor() {
        return upsertTargetDescriptor;
    }
}
