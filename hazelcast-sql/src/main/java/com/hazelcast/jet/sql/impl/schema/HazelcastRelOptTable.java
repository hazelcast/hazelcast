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

import com.hazelcast.jet.sql.impl.opt.logical.CalcIntoScanLogicalRule;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
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

/**
 * Custom implementation of Apache Calcite table.
 * <p>
 * Tables are used inside {@link TableScan} operators. During logical planning we attempt to flatten the relational tree by
 * pushing down projects and filters into the table, see {@link CalcIntoScanLogicalRule}.
 * <p>
 * It is important to distinguish {@code TableScan} operators with and without pushdown. Otherwise scans that produce different
 * results will be merged into a single equivalence group, leading to incorrect query results.
 * <p>
 * To mitigate this we provide our own table implementation that overrides {@link #getQualifiedName()} method, used for scan
 * signature calculation (see {@link TableScan#explainTerms(RelWriter)}). The overridden version adds information about
 * pushed-down projections and scans to the table name, thus avoiding the problem.
 * <p>
 * For example, a table scan over table {@code p} without pushdowns may have a signature:
 * <pre>
 * ...table=[[hazelcast, p]]....
 * </pre>
 * <p>
 * A scan over the same table with project and filter will have a signature:
 * <pre>
 * ...table=[[hazelcast, p[projects=[0, 1], filter=>($2, 1)]]]...
 * </pre>
 */
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

        List<String> res = new ArrayList<>(names);
        // Extend the table name (the last element) with project/filter signature.
        int lastElement = res.size() - 1;
        res.set(lastElement, res.get(lastElement) + getTableSignature());

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
        // Override this method to pass this table to the LogicalTableScan.
        // Otherwise the delegate would be used, which is incorrect.
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
