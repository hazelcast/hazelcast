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

package com.hazelcast.sql.impl.calcite.schema;

import com.hazelcast.core.DistributedObject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;

/**
 * Hazelcast table which can register fields dynamically.
 */
public class HazelcastTable extends AbstractTable {
    /** Name. */
    private final String name;

    /** Whether this is a partitioned map. */
    private final boolean partitioned;

    /** Data container. */
    private final DistributedObject container;

    /** Fields. */
    private final HazelcastTableFields fields = new HazelcastTableFields();

    public HazelcastTable(String name, boolean partitioned, DistributedObject container) {
        this.name = name;
        this.partitioned = partitioned;
        this.container = container;
    }

    public String getName() {
        return name;
    }

    public boolean isPartitioned() {
        return partitioned;
    }

    public boolean isReplicated() {
        return !isPartitioned();
    }

    public boolean hasContainer() {
        return container != null;
    }

    @SuppressWarnings("unchecked")
    public <T extends DistributedObject> T getContainer() {
        return (T) container;
    }

    public List<RelDataTypeField> getFieldList() {
        return fields.getFieldList();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return new HazelcastTableRelDataType(typeFactory, fields);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{name=" + name + ", partitioned=" + partitioned + '}';
    }
}
