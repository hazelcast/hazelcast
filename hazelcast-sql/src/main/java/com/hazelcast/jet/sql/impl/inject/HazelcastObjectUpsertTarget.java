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

package com.hazelcast.jet.sql.impl.inject;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.type.TypeKind;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import static com.hazelcast.jet.sql.impl.inject.UpsertTargetUtils.convertRowToJavaType;

@NotThreadSafe
class HazelcastObjectUpsertTarget implements UpsertTarget {
    private Object object;

    @Override
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        TypeKind typeKind = type.getObjectTypeKind();
        if (typeKind == TypeKind.JAVA) {
            return value -> object = convertRowToJavaType(value, type);
        } else {
            throw QueryException.error("TypeKind " + typeKind + " does not support top-level custom types");
        }
    }

    @Override
    public void init() { }

    @Override
    public Object conclude() {
        return object;
    }
}
