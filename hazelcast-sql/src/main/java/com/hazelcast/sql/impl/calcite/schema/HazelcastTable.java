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

import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.opt.cost.CostUtils;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Base class for all tables in the Calcite integration:
 * <ul>
 *     <li>Maps field types defined in the {@code core} module to Calcite types</li>
 *     <li>Provides access to the underlying table and statistics</li>
 * </ul>
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

    public List<Integer> getProjects() {
        assert projects == null || !projects.isEmpty();

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
            QueryDataType fieldType = field.getType();
            QueryDataTypeFamily fieldTypeFamily = fieldType.getTypeFamily();

            SqlTypeName sqlTypeName = SqlToQueryType.map(fieldTypeFamily);

            if (sqlTypeName == null) {
                throw new IllegalStateException("Unexpected type family: " + fieldTypeFamily);
            }

            RelDataType relDataType = typeFactory.createSqlType(sqlTypeName);
            RelDataType nullableRelDataType = typeFactory.createTypeWithNullability(relDataType, true);

            RelDataTypeField convertedField = new RelDataTypeFieldImpl(fieldName, convertedFields.size(), nullableRelDataType);
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

            double rowCount = CostUtils.adjustFilteredRowCount(statistic.getRowCount(), selectivity);

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

    public String getSignature() {
        if (projects == null && filter == null) {
            return "";
        }

        StringBuilder res = new StringBuilder("[");

        if (projects != null) {
            res.append("projects=[");

            for (int i = 0; i < projects.size(); i++) {
                if (i != 0) {
                    res.append(", ");
                }

                res.append(projects.get(i));
            }

            res.append("]");
        }

        if (filter != null) {
            if (projects != null) {
                res.append(", ");
            }

            res.append("filter=").append(filter);
        }

        res.append("]");

        return res.toString();
    }

    // TODO: VO: Make sure to project keys (and possibly collations?) properly when implementing sort/aggregate/join. Otherwise
    //  we may have incorrect optimization results.
    private final class AdjustedStatistic implements Statistic {

        private final double rowCount;

        private AdjustedStatistic(double rowCount) {
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
