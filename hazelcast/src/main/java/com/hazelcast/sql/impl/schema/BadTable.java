/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;

import java.util.Collections;
import java.util.List;

/**
 * Placeholder for a table/mapping that exists in the catalog but is not valid.
 * Allows to partially parse SQL query and generate meaningful error message
 * for the user if the mapping is used in the query.
 */
public final class BadTable extends Table {
    private final Throwable cause;

    public BadTable(String schemaName, String sqlName, Throwable cause) {
        super(schemaName, sqlName, Collections.emptyList(), new ConstantTableStatistics(0));
        this.cause = cause;
    }

    // not all methods throw to allow some parsing to occur
    @Override
    public List<TableField> getFields() {
        throw createException();
    }

    @Override
    public PlanObjectKey getObjectKey() {
        throw createException();
    }

    private QueryException createException() {
        return QueryException.error(String.format("Mapping '%s' is invalid: %s", getSqlName(), cause),
                cause);
    }
}
