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

package com.hazelcast.sql.impl.calcite.distribution;

import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.distribution.DistributionType.ANY;
import static com.hazelcast.sql.impl.calcite.distribution.DistributionType.PARTITIONED;
import static com.hazelcast.sql.impl.calcite.distribution.DistributionType.REPLICATED;
import static com.hazelcast.sql.impl.calcite.distribution.DistributionType.ROOT;

/**
 * Distribution trait. Defines how the given relation is distributed in the cluster. We define three principal
 * distribution types:
 * <ul>
 *     <li>{@code PARTITIONED} - the relation is distributed between members, and every member contains a subset of
 *     tuples. The relation may have zero, one or several distribution column groups. Every group may have one or
 *     several columns. Most often the relation will have zero or one distribution column groups. Several column
 *     groups may appear during join. E.g. given A(a) and B(b), after doing {@code A JOIN B on a = b}, resulting
 *     relation will have two distribution column groups: {a} and {b}.</li>
 *     <li>{@code REPLICATED} - the full copy of the whole relation exists on every node. Typically this distribution
 *     is used for replicated maps.</li>
 *     <li>{@code ROOT} - the whole relation exists only on a single node. This distribution type is mostly used
 *     for {@link RootPhysicalRel} operator or in case there is only one
 *     member with data in the cluster.</li>
 * </ul>
 */
// TODO: Rework to RelMultipleTrait!!
public class DistributionTrait implements RelTrait {
    /** Data is distributed between nodes, but actual distribution column is unknown. */
    public static final DistributionTrait PARTITIONED_UNKNOWN_DIST = Builder.ofType(PARTITIONED).build();

    /** Data is distributed in replicated map. */
    public static final DistributionTrait REPLICATED_DIST = Builder.ofType(REPLICATED).build();

    /** Consume the whole stream on a single node. */
    public static final DistributionTrait ROOT_DIST = Builder.ofType(ROOT).build();

    /** Distribution without any restriction. */
    public static final DistributionTrait ANY_DIST =  Builder.ofType(ANY).build();

    /** Distribution type. */
    private final DistributionType type;

    /** Distribution fields. */
    // TODO: VO: We use List<List> at the moment because there might be several distribution columns PLUS and equivalent
    //  group of columns. E.g. JOIN a.a1=b.b1 AND a.a2=b.b2 - if the relation is partitioned on [a1, a2], then it is also
    //  partitioned on [b1, b2]. IMO we should not encode the equality this way. Instead, it should be encoded inside
    //  physical operator itself, and distribution should be normalized to only a single list of columns. This will simplify
    //  planning complexity.
    private final List<List<DistributionField>> fieldGroups;

    public DistributionTrait(DistributionType type, List<List<DistributionField>> fieldGroups) {
        this.type = type;
        this.fieldGroups = fieldGroups;
    }

    public DistributionType getType() {
        return type;
    }

    public List<List<DistributionField>> getFieldGroups() {
        return fieldGroups;
    }

    public boolean hasFieldGroups() {
        return !fieldGroups.isEmpty();
    }

    @Override
    public RelTraitDef getTraitDef() {
        return DistributionTraitDef.INSTANCE;
    }

    @SuppressWarnings("checkstyle:NPathComplexity")
    @Override
    public boolean satisfies(RelTrait targetTrait) {
        if (!(targetTrait instanceof DistributionTrait)) {
            return false;
        }

        // For single-member deployments all distributions satisfy each other.
        if (HazelcastRelOptCluster.isSingleMember()) {
            return true;
        }

        DistributionTrait targetTrait0 = (DistributionTrait) targetTrait;
        DistributionType targetType = ((DistributionTrait) targetTrait).getType();

        // Any type satisfies ANY.
        if (targetType == ANY) {
            return true;
        }

        // Special handling of PARTITIONED-PARTITIONED pair.
        if (type == PARTITIONED && targetTrait0.getType() == PARTITIONED) {
            return satisfiesPartitioned(this, targetTrait0);
        }

        // Converting from REPLICATED to ROOT is always OK.
        if (type == REPLICATED && targetTrait0.getType() == ROOT) {
            return true;
        }

        // If there are no distribution fields, we may consider ROOT as a special case of PARTITIONED.
        if (type == ROOT && targetTrait0.getType() == PARTITIONED && !targetTrait0.hasFieldGroups()) {
            return true;
        }

        // Otherwise compare two distributions.
        return this.equals(targetTrait);
    }

    /**
     * Check if the first PARTITIONED trait satisfies target PARTITIONED trait.
     *
     * @param currentTrait Current trait.
     * @param targetTrait Target trait.
     * @return {@code True} if satisfies, {@code false} if conversion is required.
     */
    private static boolean satisfiesPartitioned(DistributionTrait currentTrait, DistributionTrait targetTrait) {
        assert currentTrait.getType() == PARTITIONED;
        assert targetTrait.getType() == PARTITIONED;

        if (!targetTrait.hasFieldGroups()) {
            // Converting from PARTITIONED to unknown PARTITIONED is always OK.
            return true;
        } else if (!currentTrait.hasFieldGroups()) {
            // If current distribution doesn't have distribution fields, whilst the other does, conversion if needed.
            return false;
        } else {
            // Otherwise compare every pair of source and target field group.
            for (List<DistributionField> currentGroup : currentTrait.getFieldGroups()) {
                for (List<DistributionField> targetGroup : targetTrait.getFieldGroups()) {
                    if (satisfiesPartitioned(currentGroup, targetGroup)) {
                        return true;
                    }
                }
            }

            return false;
        }
    }

    /**
     * Check if current distribution fields satisfies target distribution fields.
     *
     * This is so iff current distribution fields are a subset of target distribution fields. Order is not important.
     * 1) {a, b} satisfies {a, b}    => true
     *    {a, b} satisfies {b, a}    => true
     * 2) {a, b} satisfies {a, b, c} => true
     * 3) {a, b} satisfies {a}       => false
     *
     * @param currentFields Current distribution fields.
     * @param targetFields Target distribution fields.
     *
     * @return {@code True} if satisfies, {@code false} otherwise.
     */
    private static boolean satisfiesPartitioned(
        List<DistributionField> currentFields,
        List<DistributionField> targetFields
    ) {
        if (currentFields.size() > targetFields.size()) {
            return false;
        }

        for (DistributionField currentField : currentFields) {
            if (!targetFields.contains(currentField)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void register(RelOptPlanner planner) {
        // No-op.
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DistributionTrait other = (DistributionTrait) o;

        return type == other.type && fieldGroups.equals(other.fieldGroups);
    }

    @Override
    public int hashCode() {
        return 31 * type.hashCode() + fieldGroups.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder(type.name());

        if (fieldGroups.size() == 1) {
            appendFieldGroupToString(res, fieldGroups.get(0));
        } else if (!fieldGroups.isEmpty()) {
            res.append("{");

            for (int i = 0; i < fieldGroups.size(); i++) {
                if (i != 0) {
                    res.append(", ");
                }

                appendFieldGroupToString(res, fieldGroups.get(i));
            }

            res.append("}");
        }

        return res.toString();
    }

    private static void appendFieldGroupToString(StringBuilder builder, List<DistributionField> fields) {
        builder.append("{");

        for (int i = 0; i < fields.size(); i++) {
            if (i != 0) {
                builder.append(", ");
            }

            DistributionField field = fields.get(i);

            builder.append("$").append(field.getIndex());

            if (field.getNestedField() != null) {
                builder.append(".").append(field.getNestedField());
            }
        }

        builder.append("}");
    }

    private static String fieldGroupToString(List<DistributionField> fields) {
        StringBuilder res = new StringBuilder();

        appendFieldGroupToString(res, fields);

        return res.toString();
    }

    public static final class Builder {
        private final DistributionType type;
        private List<List<DistributionField>> fieldGroups;

        private Builder(DistributionType type) {
            assert type != null;

            this.type = type;
        }

        public static Builder ofType(DistributionType type) {
            return new Builder(type);
        }

        public Builder addFieldGroup(DistributionField field) {
            ArrayList<DistributionField> fields = new ArrayList<>(1);

            fields.add(field);

            return addFieldGroup(fields);
        }

        public Builder addFieldGroup(List<DistributionField> fields) {
            assert fields != null;
            assert !fields.isEmpty();

            if (fieldGroups == null) {
                fieldGroups = new ArrayList<>(1);
            }

            fieldGroups.add(Collections.unmodifiableList(new ArrayList<>(fields)));

            return this;
        }

        public Builder addFieldGroupPlain(List<Integer> fields) {
            List<DistributionField> fields0 = new ArrayList<>();

            for (Integer field : fields) {
                fields0.add(new DistributionField(field));
            }

            return addFieldGroup(fields0);
        }

        public DistributionTrait build() {
            if (fieldGroups == null) {
                return new DistributionTrait(type, Collections.emptyList());
            } else {
                assert !fieldGroups.isEmpty();

                // Do not modify original list to allow for builder reuse.
                ArrayList<List<DistributionField>> fieldGroups0 = new ArrayList<>(fieldGroups);

                fieldGroups0.sort(FieldGroupComparator.INSTANCE);

                return new DistributionTrait(type, Collections.unmodifiableList(fieldGroups0));
            }
        }
    }

    /**
     * Field group comparator. Sorts several distribution groups in alphabetical order, so that distribution of
     * {a1, a2} + {b1, b2} is considered equal to {b1, b2} + {a1, a2}.
     */
    private static final class FieldGroupComparator implements Comparator<List<DistributionField>> {
        /** Singleton instance. */
        private static final FieldGroupComparator INSTANCE = new FieldGroupComparator();

        @Override
        public int compare(List<DistributionField> o1, List<DistributionField> o2) {
            String str1 = fieldGroupToString(o1);
            String str2 = fieldGroupToString(o2);

            return str1.compareTo(str2);
        }
    }
}
