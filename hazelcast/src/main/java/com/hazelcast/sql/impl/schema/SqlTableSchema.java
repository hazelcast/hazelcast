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

package com.hazelcast.sql.impl.schema;

import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;

import java.util.List;

public class SqlTableSchema {
    private final String schema;
    private final String name;
    private final QueryTargetDescriptor keyDescriptor;
    private final QueryTargetDescriptor valueDescriptor;
    private final List<SqlTableField> fields;

    public SqlTableSchema(
        String schema,
        String name,
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        List<SqlTableField> fields
    ) {
        this.schema = schema;
        this.name = name;
        this.keyDescriptor = keyDescriptor;
        this.valueDescriptor = valueDescriptor;
        this.fields = fields;
    }

    public String getSchema() {
        return schema;
    }

    public String getName() {
        return name;
    }

    public QueryTargetDescriptor getKeyDescriptor() {
        return keyDescriptor;
    }

    public QueryTargetDescriptor getValueDescriptor() {
        return valueDescriptor;
    }

    public List<SqlTableField> getFields() {
        return fields;
    }
}
