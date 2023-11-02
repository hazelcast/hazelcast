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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.jet.sql.impl.connector.map.IMapSqlConnector;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;

public class PartitionedMapTable extends AbstractMapTable {

    private final List<MapTableIndex> indexes;
    private final boolean hd;
    private final List<String> partitioningAttributes;
    private final boolean supportsPartitionPruning;

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
            boolean hd,
            List<String> partitioningAttributes,
            boolean supportsPartitionPruning
    ) {
        super(
            schemaName,
            tableName,
            mapName,
            IMapSqlConnector.OBJECT_TYPE_IMAP,
            fields,
            statistics,
            keyDescriptor,
            valueDescriptor,
            keyJetMetadata,
            valueJetMetadata
        );

        this.indexes = indexes;
        this.hd = hd;
        this.partitioningAttributes = partitioningAttributes;
        this.supportsPartitionPruning = supportsPartitionPruning;
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
                isHd(),
                partitioningAttributes(),
                supportsPartitionPruning());
    }

    public List<MapTableIndex> getIndexes() {
        checkException();

        return indexes != null ? indexes : emptyList();
    }

    public boolean isHd() {
        return hd;
    }

    public Stream<MapTableField> keyFields() {
        return getFields().stream().map(field -> (MapTableField) field).filter(field -> field.getPath().isKey());
    }

    public Stream<MapTableField> valueFields() {
        return getFields().stream().map(field -> (MapTableField) field).filter(field -> !field.getPath().isKey());
    }

    public QueryPath[] paths() {
        return getFields().stream().map(field -> ((MapTableField) field).getPath()).toArray(QueryPath[]::new);
    }

    public QueryDataType[] types() {
        return getFields().stream().map(TableField::getType).toArray(QueryDataType[]::new);
    }

    public QueryPath[] valuePaths() {
        return valueFields().map(MapTableField::getPath).toArray(QueryPath[]::new);
    }

    public QueryDataType[] valueTypes() {
        return valueFields().map(TableField::getType).toArray(QueryDataType[]::new);
    }

    /**
     * Returns list of Partitioning Attribute names if the corresponding map uses AttributePartitioningStrategy,
     * otherwise returns an empty list. Note that attribute names are not the same as column names as key fields
     * can be renamed during Mapping creation.
     *
     * @return list of partitioning attribute names or empty list.
     */
    public List<String> partitioningAttributes() {
        return partitioningAttributes != null ? partitioningAttributes : emptyList();
    }

    /**
     * Flag to indicate whether underlying Map/Table uses one of the supported PartitioningStrategies and therefore
     * can support Partition Pruning.
     * @return true if table supports Partition Pruning
     */
    public boolean supportsPartitionPruning() {
        return supportsPartitionPruning;
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
        private final List<String> partitioningAttributes;
        private final boolean supportsPartitionPruning;

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
                boolean hd,
                final List<String> partitioningAttributes,
                final boolean supportsPartitionPruning) {
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
            this.partitioningAttributes = partitioningAttributes;
            this.supportsPartitionPruning = supportsPartitionPruning;
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
                    && conflictingSchemas.equals(that.conflictingSchemas)
                    && partitioningAttributes.equals(that.partitioningAttributes)
                    && supportsPartitionPruning == that.supportsPartitionPruning;
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
            result = 31 * result + partitioningAttributes.hashCode();
            result = 31 * result + (supportsPartitionPruning ? 1 : 0);
            return result;
        }
    }
}
