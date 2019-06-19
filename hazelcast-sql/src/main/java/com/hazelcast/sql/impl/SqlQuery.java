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

package com.hazelcast.sql.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.IMap;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.BindableRel;
import org.apache.calcite.interpreter.Node;
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.interpreter.Sink;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Collection;

public final class SqlQuery extends AbstractRelNode implements BindableRel {

    private final SqlTable sqlTable;

    private final RelOptTable table;

    private final Filter filter;

    private final Project project;

    private final Aggregate aggregate;

    public SqlQuery(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, SqlTable sqlTable) {
        this(cluster, traitSet, table, sqlTable, null, null, null);
    }

    private SqlQuery(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, SqlTable sqlTable, Filter filter,
                     Project project, Aggregate aggregate) {
        super(cluster, traitSet);
        assert project == null || aggregate == null;
        this.table = table;
        this.sqlTable = sqlTable;
        this.filter = filter;
        this.project = project;
        this.aggregate = aggregate;
    }

    public SqlQuery tryToCombineWith(Filter filter) {
        if (this.filter != null || !Filters.isSupported(filter)) {
            return null;
        }

        return new SqlQuery(getCluster(), getTraitSet(), getTable(), sqlTable, filter, project, aggregate);
    }

    public SqlQuery tryToCombineWith(Project project) {
        if (this.project != null || aggregate != null || !Projects.isSupported(project)) {
            return null;
        }

        return new SqlQuery(getCluster(), getTraitSet(), getTable(), sqlTable, filter, project, null);
    }

    public SqlQuery tryToCombineWith(Aggregate aggregate) {
        if (this.aggregate != null || project != null || !Aggregates.isSupported(aggregate)) {
            return null;
        }

        return new SqlQuery(getCluster(), getTraitSet(), getTable(), sqlTable, filter, null, aggregate);
    }

    @Override
    public void register(RelOptPlanner planner) {
        super.register(planner);
        planner.addRule(SqlFilterRule.INSTANCE);
        planner.addRule(SqlProjectRule.INSTANCE);
        planner.addRule(SqlAggregateRule.INSTANCE);
    }

    @Override
    protected RelDataType deriveRowType() {
        if (project != null) {
            return project.getRowType();
        } else if (aggregate != null) {
            return aggregate.getRowType();
        } else {
            return table.getRowType();
        }
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double rowCount;
        if (aggregate != null) {
            if (Aggregates.isDistinct(aggregate)) {
                rowCount = 750.0;
            } else {
                return 1.0;
            }
        } else {
            rowCount = 1000.0;
        }

        return filter == null ? rowCount : rowCount / 2.0;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowsToTouch = 1000.0;
        double rowsToReturn = estimateRowCount(mq);
        int fieldCount = getRowType().getFieldCount();

        double cpu = rowsToTouch * fieldCount;
        double io = rowsToReturn * fieldCount;
        return planner.getCostFactory().makeCost(rowsToTouch, cpu, io);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("table", table.getQualifiedName());
        if (filter != null) {
            pw.item("filter", filter.getCondition());
        }
        if (project != null) {
            pw.item("projects", project.getProjects());
        }
        if (aggregate != null) {
            pw.item("aggregate", aggregate.getAggCallList());
        }
        return pw;
    }

    @Override
    public RelOptTable getTable() {
        return table;
    }

    @Override
    public Enumerable<Object[]> bind(DataContext dataContext) {
        return performQuery();
    }

    @Override
    public Class<Object[]> getElementType() {
        return Object[].class;
    }

    @Override
    public Node implement(InterpreterImplementor implementor) {
        // This method is used only during the fallback execution for
        // unsupported queries and their parts.

        Sink sink = implementor.compiler.sink(this);
        return () -> {
            Enumerable<Object[]> enumerable = performQuery();
            Enumerator<Object[]> enumerator = enumerable.enumerator();
            while (enumerator.moveNext()) {
                sink.send(Row.of(enumerator.current()));
            }
            sink.end();
        };
    }

    private Enumerable<Object[]> performQuery() {
        IMap map = sqlTable.getMap();

        Predicate predicate = filter == null ? null : Filters.convert(filter);

        if (project != null) {
            return performProjection(map, project.getRowType(), predicate);
        } else if (aggregate != null) {
            return performAggregation(map, aggregate, predicate);
        }

        return performProjection(map, table.getRowType(), predicate);
    }

    @SuppressWarnings("unchecked")
    private static Enumerable<Object[]> performProjection(IMap map, RelDataType rowType, Predicate predicate) {
        Projection projection = Projects.convert(rowType);
        if (rowType.getFieldCount() == 1) {
            Collection rows = predicate == null ? map.project(projection) : map.project(projection, predicate);
            return new SqlArrayEnumerableCollection(rows);
        } else {
            Collection<Object[]> rows = predicate == null ? map.project(projection) : map.project(projection, predicate);
            return new SqlEnumerableCollection<>(rows);
        }
    }

    @SuppressWarnings("unchecked")
    private static Enumerable<Object[]> performAggregation(IMap map, Aggregate aggregate, Predicate predicate) {
        Aggregator aggregator = Aggregates.convert(aggregate);
        Object result = predicate == null ? map.aggregate(aggregator) : map.aggregate(aggregator, predicate);
        if (result instanceof Collection) {
            return new SqlArrayEnumerableCollection((Collection) result);
        } else {
            return Linq4j.singletonEnumerable(new Object[]{result});
        }
    }

}
