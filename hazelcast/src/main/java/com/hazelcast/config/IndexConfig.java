/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

// TODO: From Matko:
// TODO: https://github.com/hazelcast/hazelcast/blob/108939ae3c5077d91adc134d87620dde5990cfeb/hazelcast/src/main/resources/hazelcast-config-4.0.xsd#L1743-L1747
// TODO: https://github.com/hazelcast/hazelcast/blob/108939ae3c5077d91adc134d87620dde5990cfeb/hazelcast/src/main/resources/hazelcast-config-4.0.xsd#L3590-L3650

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableList;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Base class for index configurations. Hazelcast support two types of indexes: sorted index and hash index.
 * Use their specific classes for configuration.
 *
 * @see com.hazelcast.config.IndexType
 * @see com.hazelcast.config.IndexColumn
 * @see com.hazelcast.config.MapConfig#setIndexConfigs(List)
 */
public class IndexConfig implements IdentifiedDataSerializable {
    /** Default index type. */
    public static final IndexType DEFAULT_TYPE = IndexType.SORTED;

    /** Name of the index. */
    private String name;

    /** Type of the index. */
    private IndexType type = DEFAULT_TYPE;

    /** Indexed columns. */
    private List<IndexColumn> columns;

    public IndexConfig() {
        // No-op.
    }

    /**
     * Gets name of the index.
     *
     * @return Name of the index or {@code null} if index name should be generated automatically.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets name of the index.
     *
     * @param name Name of the index or {@code null} if index name should be generated automatically.
     * @return This instance for chaining.
     */
    public IndexConfig setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * Gets type of the index.
     * <p>
     * Defaults to {@link IndexType#SORTED}.
     *
     * @return Type of the index.
     */
    public IndexType getType() {
        return type;
    }

    /**
     * Sets type of the index.
     * <p>
     * Defaults to {@link IndexType#SORTED}.
     *
     * @param type Type of the index.
     * @return This instance for chaining.
     */
    public IndexConfig setType(IndexType type) {
        this.type = checkNotNull(type, "Index type cannot be null.");;

        return this;
    }

    /**
     * Gets index columns.
     *
     * @return Index columns.
     */
    public List<IndexColumn> getColumns() {
        if (columns == null)
            columns = new ArrayList<>();

        return columns;
    }

    /**
     * Adds an index column.
     *
     * @param column Index column.
     * @return This instance for chaining.
     */
    public IndexConfig addColumn(IndexColumn column) {
        getColumns().add(column);

        return this;
    }

    /**
     * Sets index columns.
     *
     * @param columns Index columns.
     * @return This instance for chaining.
     */
    public IndexConfig setColumns(List<IndexColumn> columns) {
        if (columns == null || columns.isEmpty())
            columns = null;
        else
            columns = new ArrayList<>(columns);

        this.columns = columns;

        return this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.INDEX_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        writeNullableList(columns, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        columns = readNullableList(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        IndexConfig that = (IndexConfig) o;

        if (name != null ? name.equals(that.name) : that.name == null)
            return false;

        return getColumns().equals(that.getColumns());
    }

    @Override
    public int hashCode() {
        int result = (name != null ? name.hashCode() : 0);

        result = 31 * result + getColumns().hashCode();

        return result;
    }

    @Override
    public String toString() {
        return "IndexConfig{name=" + name + ", type=" + type + ", columns=" + getColumns() + '}';
    }
}
