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

package com.hazelcast.sql.impl.calcite.schema;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

public class HazelcastRelOptTable implements Prepare.PreparingTable {

    private final Prepare.PreparingTable delegate;

    public HazelcastRelOptTable(Prepare.PreparingTable delegate) {
        this.delegate = delegate;
    }

    public Prepare.PreparingTable getDelegate() {
        return delegate;
    }

    @Override
    public List<String> getQualifiedName() {
        // Get original names.
        List<String> names = delegate.getQualifiedName();

        assert names != null && !names.isEmpty();

        // Extend table name with project/filter information.
        List<String> res = new ArrayList<>(names.size());

        for (int i = 0; i < names.size(); i++) {
            String currentPart = names.get(i);

            if (i != names.size() - 1) {
                // Not a table name, just add it as is.
                res.add(currentPart);
            } else {
                // Table name. Extend it with project/filter.
                res.add(currentPart + getTableSignature());
            }
        }

        return res;
    }

    private String getTableSignature() {
        HazelcastTable table = delegate.unwrap(HazelcastTable.class);

        assert table != null;

        return table.getSignature();
    }

    @Override
    public double getRowCount() {
        return delegate.getRowCount();
    }

    @Override
    public RelDataType getRowType() {
        return delegate.getRowType();
    }

    @Override
    public RelOptSchema getRelOptSchema() {
        return delegate.getRelOptSchema();
    }

    @Override
    public RelNode toRel(ToRelContext context) {
        return LogicalTableScan.create(context.getCluster(), this, context.getTableHints());
    }

    @Override
    public List<RelCollation> getCollationList() {
        return delegate.getCollationList();
    }

    @Override
    public RelDistribution getDistribution() {
        return delegate.getDistribution();
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
        return delegate.isKey(columns);
    }

    @Override
    public List<ImmutableBitSet> getKeys() {
        return delegate.getKeys();
    }

    @Override
    public List<RelReferentialConstraint> getReferentialConstraints() {
        return delegate.getReferentialConstraints();
    }

    @Override
    public Expression getExpression(Class clazz) {
        return delegate.getExpression(clazz);
    }

    @Override
    public RelOptTable extend(List<RelDataTypeField> extendedFields) {
        return delegate.extend(extendedFields);
    }

    @Override
    public List<ColumnStrategy> getColumnStrategies() {
        return delegate.getColumnStrategies();
    }

    @Override
    public SqlMonotonicity getMonotonicity(String columnName) {
        return delegate.getMonotonicity(columnName);
    }

    @Override
    public SqlAccessType getAllowedAccess() {
        return delegate.getAllowedAccess();
    }

    @Override
    public boolean supportsModality(SqlModality modality) {
        return delegate.supportsModality(modality);
    }

    @Override
    public boolean isTemporal() {
        return delegate.isTemporal();
    }

    @SuppressWarnings("deprecation")
    @Override
    public boolean columnHasDefaultValue(RelDataType rowType, int ordinal, InitializerContext initializerContext) {
        return delegate.columnHasDefaultValue(rowType, ordinal, initializerContext);
    }

    @Override
    public <C> C unwrap(Class<C> aClass) {
        return delegate.unwrap(aClass);
    }
}
