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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;

import java.util.List;

/**
 * Table backed by IMap or ReplicatedMap.
 */
public abstract class AbstractMapTable extends Table {

    private final QueryTargetDescriptor keyDescriptor;
    private final QueryTargetDescriptor valueDescriptor;
    private final QueryException exception;

    protected AbstractMapTable(
        String schemaName,
        String name,
        List<TableField> fields,
        TableStatistics statistics,
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor
    ) {
        super(schemaName, name, fields, statistics);

        this.keyDescriptor = keyDescriptor;
        this.valueDescriptor = valueDescriptor;

        exception = null;
    }

    protected AbstractMapTable(String schemaName, String name, QueryException exception) {
        super(schemaName, name, null, new ConstantTableStatistics(0));

        this.keyDescriptor = null;
        this.valueDescriptor = null;

        this.exception = exception;
    }

    @Override
    public int getFieldCount() {
        checkException();

        return super.getFieldCount();
    }

    @Override
    public <T extends TableField> T getField(int index) {
        checkException();

        return super.getField(index);
    }

    public QueryTargetDescriptor getKeyDescriptor() {
        return keyDescriptor;
    }

    public QueryTargetDescriptor getValueDescriptor() {
        return valueDescriptor;
    }

    public QueryException getException() {
        return exception;
    }

    protected void checkException() {
        if (exception != null) {
            throw exception;
        }
    }
}
