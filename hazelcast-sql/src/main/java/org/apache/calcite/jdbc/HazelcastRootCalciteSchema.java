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

package org.apache.calcite.jdbc;

import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import org.apache.calcite.schema.SchemaVersion;

/**
 * Root Calcite schema.
 * <p>
 * Calcite uses {@link org.apache.calcite.schema.Schema} to store actual objects, and
 * {@link org.apache.calcite.jdbc.CalciteSchema} as a wrapper. This class is a straightforward implementation of the latter.
 * <p>
 * Located in the Calcite package because the required super constructor is package-private.
 */
public final class HazelcastRootCalciteSchema extends SimpleCalciteSchema {
    public HazelcastRootCalciteSchema(HazelcastSchema schema) {
        super(null, schema, "");
    }

    @Override
    public CalciteSchema createSnapshot(SchemaVersion version) {
        return this;
    }
}
