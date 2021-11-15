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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.sql.impl.expression.json.JsonParseFunction;
import com.hazelcast.jet.sql.impl.expression.json.JsonQueryFunction;
import com.hazelcast.jet.sql.impl.expression.json.JsonValueFunction;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlSerializationFactory;

public class JetSqlSerializationFactory implements SqlSerializationFactory {
    @Override
    public IdentifiedDataSerializable createJsonQueryFunction() {
        return new JsonQueryFunction();
    }

    @Override
    public IdentifiedDataSerializable createJsonValueFunction() {
        return new JsonValueFunction<>();
    }

    @Override
    public IdentifiedDataSerializable createJsonParseFunction() {
        return new JsonParseFunction();
    }
}
