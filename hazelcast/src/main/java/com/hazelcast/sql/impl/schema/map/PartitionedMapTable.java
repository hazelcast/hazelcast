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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PARTITIONED;

public class PartitionedMapTable extends AbstractMapTable {

    private final List<MapTableIndex> indexes;
    private final boolean hd;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public PartitionedMapTable(
            String schemaName,
            String tableName,
            String mapName,
            List<TableField> fields,
            TableStatistics statistics,
            QueryTargetDescriptor keyDescriptor,
            QueryTargetDescriptor valueDescriptor,
            Object keyJetMetadata,
            Object valueJetMetadata,
            List<MapTableIndex> indexes,
            boolean hd
    ) {
        super(
            schemaName,
            tableName,
            mapName,
            fields,
            statistics,
            keyDescriptor,
            valueDescriptor,
            keyJetMetadata,
            valueJetMetadata
        );

        this.indexes = indexes;
        this.hd = hd;
    }

    public PartitionedMapTable(String name, QueryException exception) {
        super(SCHEMA_NAME_PARTITIONED, name, exception);

        this.indexes = null;
        this.hd = false;
    }

    @Override
    public PlanObjectKey getObjectKey() {
        if (!isValid()) {
            return null;
        }

        return new PartitionedMapPlanObjectKey(
            getSchemaName(),
            getSqlName(),
            getMapName(),
            getFields(),
            getConflictingSchemas(),
            getKeyDescriptor(),
            getValueDescriptor(),
            getKeyJetMetadata(),
            getValueJetMetadata(),
            getIndexes(),
            isHd()
        );
    }

    public List<MapTableIndex> getIndexes() {
        checkException();

        return indexes != null ? indexes : Collections.emptyList();
    }

    public boolean isHd() {
        return hd;
    }

    static class PartitionedMapPlanObjectKey implements PlanObjectKey {

        private final String schemaName;
        private final String tableName;
        private final String mapName;
        private final List<TableField> fields;
        private final QueryTargetDescriptor keyDescriptor;
        private final QueryTargetDescriptor valueDescriptor;
        private final Object keyJetMetadata;
        private final Object valueJetMetadata;
        private final List<MapTableIndex> indexes;
        private final boolean hd;
        private final Set<String> conflictingSchemas;

        @SuppressWarnings("checkstyle:ParameterNumber")
        PartitionedMapPlanObjectKey(
                String schemaName,
                String tableName,
                String mapName,
                List<TableField> fields,
                Set<String> conflictingSchemas,
                QueryTargetDescriptor keyDescriptor,
                QueryTargetDescriptor valueDescriptor,
                Object keyJetMetadata,
                Object valueJetMetadata,
                List<MapTableIndex> indexes,
                boolean hd
        ) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.mapName = mapName;
            this.fields = fields;
            this.keyDescriptor = keyDescriptor;
            this.valueDescriptor = valueDescriptor;
            this.keyJetMetadata = keyJetMetadata;
            this.valueJetMetadata = valueJetMetadata;
            this.indexes = indexes;
            this.hd = hd;
            this.conflictingSchemas = conflictingSchemas;
        }

        @Override
        @SuppressWarnings("checkstyle:cyclomaticcomplexity")
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            PartitionedMapPlanObjectKey that = (PartitionedMapPlanObjectKey) o;

            return hd == that.hd
                    && schemaName.equals(that.schemaName)
                    && tableName.equals(that.tableName)
                    && mapName.equals(that.mapName)
                    && fields.equals(that.fields)
                    && keyDescriptor.equals(that.keyDescriptor)
                    && valueDescriptor.equals(that.valueDescriptor)
                    && Objects.equals(keyJetMetadata, that.keyJetMetadata)
                    && Objects.equals(valueJetMetadata, that.valueJetMetadata)
                    && indexes.equals(that.indexes)
                    && conflictingSchemas.equals(that.conflictingSchemas);
        }

        @Override
        public int hashCode() {
            int result = schemaName.hashCode();
            result = 31 * result + tableName.hashCode();
            result = 31 * result + mapName.hashCode();
            result = 31 * result + fields.hashCode();
            result = 31 * result + keyDescriptor.hashCode();
            result = 31 * result + valueDescriptor.hashCode();
            result = 31 * result + Objects.hashCode(keyJetMetadata);
            result = 31 * result + Objects.hashCode(valueJetMetadata);
            result = 31 * result + indexes.hashCode();
            result = 31 * result + (hd ? 1 : 0);
            result = 31 * result + conflictingSchemas.hashCode();
            return result;
        }
    }
}
