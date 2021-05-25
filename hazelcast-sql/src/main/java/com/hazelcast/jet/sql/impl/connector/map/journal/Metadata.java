/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.map.journal;

import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;

import java.util.List;

public class Metadata {
    private final List<TableField> fields;
    private final QueryTargetDescriptor keyQueryTargetDescriptor;
    private final QueryTargetDescriptor valueQueryTargetDescriptor;

    public Metadata(
            List<TableField> fields,
            QueryTargetDescriptor keyQueryTargetDescriptor,
            QueryTargetDescriptor valueQueryTargetDescriptor
    ) {
        this.fields = fields;
        this.keyQueryTargetDescriptor = keyQueryTargetDescriptor;
        this.valueQueryTargetDescriptor = valueQueryTargetDescriptor;
    }

    public List<TableField> getFields() {
        return fields;
    }

    public QueryTargetDescriptor getKeyQueryTargetDescriptor() {
        return keyQueryTargetDescriptor;
    }

    public QueryTargetDescriptor getValueQueryTargetDescriptor() {
        return valueQueryTargetDescriptor;
    }
}
