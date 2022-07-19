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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.sql.impl.expression.RowValue;
import com.hazelcast.sql.impl.schema.type.TypeKind;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;

import static com.hazelcast.jet.sql.impl.inject.UpsertTargetUtils.convertRowToCompactType;
import static com.hazelcast.jet.sql.impl.inject.UpsertTargetUtils.convertRowToJavaType;

public class HazelcastObjectUpsertTarget implements UpsertTarget {
    private Object object;

    public HazelcastObjectUpsertTarget() { }

    public HazelcastObjectUpsertTarget(final QueryDataType queryDataType) {
        // TODO precompute injectors and perform checks
    }

    @Override
    public UpsertInjector createInjector(@Nullable final String path, final QueryDataType queryDataType) {
        return value -> {
            switch (TypeKind.of(queryDataType.getObjectTypeKind())) {
                case JAVA:
                    this.object = convertRowToJavaType(value, queryDataType);
                    break;
                case COMPACT:
                    this.object = convertRowToCompactType((RowValue) value, queryDataType);
                    break;
                default:
                    break;
            }
        };
    }

    @Override
    public void init() {

    }

    @Override
    public Object conclude() {
        return object;
    }
}
