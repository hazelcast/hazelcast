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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import java.util.Objects;

public abstract class HazelcastTableSourceFunction extends HazelcastTableFunction {

    private final SqlConnector connector;

    protected HazelcastTableSourceFunction(
            String name,
            SqlOperandMetadata operandMetadata,
            SqlReturnTypeInference returnTypeInference,
            SqlConnector connector
    ) {
        super(name, operandMetadata, returnTypeInference);

        this.connector = Objects.requireNonNull(connector);
    }

    public final boolean isStream() {
        return connector.isStream();
    }
}
