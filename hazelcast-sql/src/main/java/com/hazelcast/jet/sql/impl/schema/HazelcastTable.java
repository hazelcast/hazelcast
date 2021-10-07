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
import com.hazelcast.jet.sql.impl.opt.logical.FilterIntoScanLogicalRule;
import com.hazelcast.jet.sql.impl.opt.logical.ProjectIntoScanLogicalRule;
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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nonnull;
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
 * filters into the table using {@link ProjectIntoScanLogicalRule} and {@link FilterIntoScanLogicalRule}. These rules
 * reduce the amount of data returned from the table during scanning. Pushed-down projection ensures that only columns required
 * by parent operators are returned, thus implementing field trimming. Pushed-down filter reduces the number of returned rows.
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
    private final List<Integer> projects;
    private final RexNode filter;

    private RelDataType rowType;
    private Set<String> hiddenFieldNames;

    public HazelcastTable(Table target, Statistic statistic) {
        this(target, statistic, null, null);
    }

    private HazelcastTable(Table target, Statistic statistic, List<Integer> projects, RexNode filter) {
        this.target = target;
        this.statistic = statistic;
        this.projects = projects;
        this.filter = filter;
    }

    public HazelcastTable withProject(List<Integer> projects) {
        return new HazelcastTable(target, statistic, projects, filter);
    }

    public HazelcastTable withFilter(RexNode filter) {
        return new HazelcastTable(target, statistic, projects, filter);
    }

    @Nonnull
    public List<Integer> getProjects() {
        if (projects == null) {
            int fieldCount = target.getFieldCount();

            List<Integer> res = new ArrayList<>(fieldCount);

            for (int i = 0; i < fieldCount; i++) {
                res.add(i);
            }

            return res;
        }

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
        if (rowType != null) {
            return rowType;
        }

        hiddenFieldNames = new HashSet<>();

        List<Integer> projects = getProjects();

        List<RelDataTypeField> convertedFields = new ArrayList<>(projects.size());

        for (Integer project : projects) {
            TableField field = target.getField(project);

            String fieldName = field.getName();

            RelDataType relType = OptUtils.convert(field, typeFactory);

            RelDataTypeField convertedField = new RelDataTypeFieldImpl(fieldName, convertedFields.size(), relType);
            convertedFields.add(convertedField);

            if (field.isHidden()) {
                hiddenFieldNames.add(fieldName);
            }
        }

        rowType = new RelRecordType(StructKind.PEEK_FIELDS, convertedFields, false);

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
        assert hiddenFieldNames != null;

        return hiddenFieldNames.contains(fieldName);
    }

    public int getOriginalFieldCount() {
        return target.getFieldCount();
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
