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

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.cost.CostUtils;
import com.hazelcast.jet.sql.impl.opt.logical.CalcIntoScanLogicalRule;
import com.hazelcast.jet.sql.impl.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import static java.util.stream.Collectors.joining;

/**
 * Base class for all tables in the Calcite integration:
 * <ul>
 *     <li>Maps field types defined in the {@code core} module to Calcite types</li>
 *     <li>Provides access to the underlying table and statistics</li>
 *     <li>Encapsulates projects and filter to allow for constrained scans</li>
 * </ul>
 * <p>
 * <h2>Constrained scans</h2>
 * For a sequence of logical project/filter/scan operators we would like to ensure that the resulting relational tree is as
 * flat as possible because this minimizes the processing overhead and memory usage. To achieve this we try to push projects and
 * filters into the table using {@link CalcIntoScanLogicalRule}. These rules reduce the amount of data returned from the table
 * during scanning. Pushed-down projection ensures that only columns required by parent operators are returned, thus
 * implementing field trimming. Pushed-down filter reduces the number of returned rows.
 * <p>
 * Projects are indexes of table fields that are returned. Initial projection (i.e. before optimization) returns all the columns.
 * After project pushdown the number and order of columns may change. For example, for the table {@code t[f0, f1, f2]} the
 * initial projection is {@code [0, 1, 2]}. After pushdown of a {@code "SELECT f2, f0"} the projection becomes {@code [2, 0]}
 * which means that the columns {@code [f2, f0]} are returned, in that order.
 * <p>
 * Filter is a conjunctive expression that references table fields via their original indexes. That is, {@code [f2]} is
 * referenced as {@code [2]} even if it is projected as the first field in the example above. This is needed to allow for
 * projections and filters on disjoint sets of attributes.
 * <p>
 * Consider the following SQL statement:
 * <pre>
 * SELECT f2, f0 FROM t WHERE f1 > ?
 * </pre>
 * In this case {@code projects=[2, 0]}, {@code filter=[>$1, ?]}.
 * <p>
 * We do not pushdown the project expressions other than columns, because expressions inside the scan may change its physical
 * properties, thus making further optimization more complex.
 */
public class HazelcastTable extends AbstractTable {

    private final Table target;
    private final Statistic statistic;
    private final RexNode filter;
    private List<RexNode> projects;

    private RelDataType rowType;
    private final Set<String> hiddenFieldNames = new HashSet<>();

    public HazelcastTable(Table target, Statistic statistic) {
        this.target = target;
        this.statistic = statistic;
        this.filter = null;
    }

    private HazelcastTable(
            Table target,
            Statistic statistic,
            @Nonnull List<RexNode> projects,
            @Nullable RelDataType rowType,
            @Nullable RexNode filter
    ) {
        this.target = target;
        this.statistic = statistic;
        this.projects = projects;
        this.rowType = rowType == null ? computeRowType(projects) : rowType;
        this.filter = filter;
    }

    private void initRowType() {
        if (rowType == null) {
            // produce default projects
            int fieldCount = target.getFieldCount();
            projects = new ArrayList<>(fieldCount);
            for (int i = 0; i < fieldCount; i++) {
                TableField field = target.getField(i);
                RelDataType type = OptUtils.convert(field, HazelcastTypeFactory.INSTANCE);
                projects.add(new RexInputRef(i, type));
            }
            rowType = computeRowType(projects);
        }
    }

    public HazelcastTable withProject(List<RexNode> projects, @Nullable RelDataType rowType) {
        return new HazelcastTable(target, statistic, projects, rowType, filter);
    }

    public HazelcastTable withFilter(RexNode filter) {
        return new HazelcastTable(target, statistic, projects, rowType, filter);
    }

    @Nonnull
    public List<RexNode> getProjects() {
        initRowType();
        return projects;
    }

    public RexNode getFilter() {
        return filter;
    }

    @SuppressWarnings("unchecked")
    public <T extends Table> T getTarget() {
        return (T) target;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        initRowType();
        return rowType;
    }

    @Override
    public Statistic getStatistic() {
        if (filter == null) {
            return statistic;
        } else {
            Double selectivity = RelMdUtil.guessSelectivity(filter);
            Double rowCount = CostUtils.adjustFilteredRowCount(statistic.getRowCount(), selectivity);
            return new AdjustedStatistic(rowCount);
        }
    }

    public double getTotalRowCount() {
        return statistic.getRowCount();
    }

    public boolean isHidden(String fieldName) {
        return hiddenFieldNames.contains(fieldName);
    }

    /**
     * Constructs a signature for the table.
     * <p>
     * See {@link HazelcastRelOptTable} for more information.
     *
     * @return Signature.
     */
    public String getSignature() {
        StringJoiner res = new StringJoiner(", ", "[", "]");
        res.setEmptyValue("");
        res.add("projects=" + getProjects().stream().map(Objects::toString).collect(joining(", ", "[", "]")));
        if (filter != null) {
            res.add("filter=" + filter);
        }
        return res.toString();
    }

    private RelDataType computeRowType(List<RexNode> projects) {
        List<RelDataTypeField> typeFields = new ArrayList<>(projects.size());
        for (int i = 0; i < projects.size(); i++) {
            RexNode project = projects.get(i);
            RelDataTypeField fieldType;
            if (project instanceof RexInputRef) {
                TableField field = target.getField(((RexInputRef) project).getIndex());
                fieldType = new RelDataTypeFieldImpl(field.getName(), i, project.getType());
                if (field.isHidden()) {
                    hiddenFieldNames.add(field.getName());
                }
            } else {
                fieldType = new RelDataTypeFieldImpl("EXPR$" + i, i, project.getType());
            }

            typeFields.add(fieldType);
        }
        return new RelRecordType(StructKind.PEEK_FIELDS, typeFields, false);
    }

    /**
     * Statistics that takes into account the row count after the filter is applied.
     */
    private final class AdjustedStatistic implements Statistic {

        private final Double rowCount;

        private AdjustedStatistic(Double rowCount) {
            this.rowCount = rowCount;
        }

        @Override
        public Double getRowCount() {
            return rowCount;
        }

        @Override
        public boolean isKey(ImmutableBitSet columns) {
            return statistic.isKey(columns);
        }

        @Override
        public List<ImmutableBitSet> getKeys() {
            return statistic.getKeys();
        }

        @Override
        public List<RelReferentialConstraint> getReferentialConstraints() {
            return statistic.getReferentialConstraints();
        }

        @Override
        public List<RelCollation> getCollations() {
            return statistic.getCollations();
        }

        @Override
        public RelDistribution getDistribution() {
            return statistic.getDistribution();
        }
    }
}
