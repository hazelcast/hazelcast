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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Table backed by IMap or ReplicatedMap.
 */
public abstract class AbstractMapTable extends Table {

    private final String mapName;
    private final QueryTargetDescriptor keyDescriptor;
    private final QueryTargetDescriptor valueDescriptor;
    private final Object keyJetMetadata;
    private final Object valueJetMetadata;
    private final QueryException exception;

    /**
     * @param sqlName Name of the table as it appears in the SQL
     * @param mapName Name of the underlying map
     */
    protected AbstractMapTable(
        String schemaName,
        String sqlName,
        String mapName,
        List<TableField> fields,
        TableStatistics statistics,
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        Object keyJetMetadata,
        Object valueJetMetadata
    ) {
        super(schemaName, sqlName, fields, statistics);

        this.mapName = requireNonNull(mapName);
        this.keyDescriptor = keyDescriptor;
        this.valueDescriptor = valueDescriptor;
        this.keyJetMetadata = keyJetMetadata;
        this.valueJetMetadata = valueJetMetadata;

        exception = null;
    }

    protected AbstractMapTable(String schemaName, String name, QueryException exception) {
        super(schemaName, name, Collections.emptyList(), new ConstantTableStatistics(0));

        this.mapName = name;
        this.keyDescriptor = null;
        this.valueDescriptor = null;
        this.keyJetMetadata = null;
        this.valueJetMetadata = null;

        this.exception = exception;
    }

    /**
     * The name of the underlying map.
     */
    @Nonnull
    public String getMapName() {
        return mapName;
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

    protected boolean isValid() {
        return exception == null;
    }

    public QueryTargetDescriptor getKeyDescriptor() {
        return keyDescriptor;
    }

    public QueryTargetDescriptor getValueDescriptor() {
        return valueDescriptor;
    }

    public Object getKeyJetMetadata() {
        return keyJetMetadata;
    }

    public Object getValueJetMetadata() {
        return valueJetMetadata;
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
