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

package com.hazelcast.sql.impl.calcite.opt.distribution;

import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;

import java.util.Comparator;
import java.util.List;

import static com.hazelcast.sql.impl.calcite.opt.distribution.DistributionType.ANY;
import static com.hazelcast.sql.impl.calcite.opt.distribution.DistributionType.PARTITIONED;
import static com.hazelcast.sql.impl.calcite.opt.distribution.DistributionType.REPLICATED;
import static com.hazelcast.sql.impl.calcite.opt.distribution.DistributionType.ROOT;

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
    /** Trait definition. */
    private final DistributionTraitDef traitDef;

    /** Distribution type. */
    private final DistributionType type;

    /** Distribution fields. */
    // TODO: VO: We use List<List> at the moment because there might be several distribution columns PLUS and equivalent
    //  group of columns. E.g. JOIN a.a1=b.b1 AND a.a2=b.b2 - if the relation is partitioned on [a1, a2], then it is also
    //  partitioned on [b1, b2]. IMO we should not encode the equality this way. Instead, it should be encoded inside
    //  physical operator itself, and distribution should be normalized to only a single list of columns. This will simplify
    //  planning complexity.
    private final List<List<Integer>> fieldGroups;

    DistributionTrait(DistributionTraitDef traitDef, DistributionType type, List<List<Integer>> fieldGroups) {
        this.traitDef = traitDef;
        this.type = type;
        this.fieldGroups = fieldGroups;
    }

    public DistributionType getType() {
        return type;
    }

    public List<List<Integer>> getFieldGroups() {
        return fieldGroups;
    }

    public boolean hasFieldGroups() {
        return !fieldGroups.isEmpty();
    }

    public int getMemberCount() {
        return traitDef.getMemberCount();
    }

    /**
     * Check if the result set of the node having this trait is guaranteed to exist on all members that will execute a
     * fragment with this node.
     *
     * @return {@code true} if the full result set exists on all participants of the fragment hosting this node.
     */
    @SuppressWarnings("RedundantIfStatement")
    public boolean isFullResultSetOnAllParticipants() {
        if (traitDef.getMemberCount() == 1) {
            // If the plan is created for a single member, then the condition is true by definition.
            return true;
        }

        if (type == ROOT) {
            // Root fragment is always executed on a single member.
            return true;
        }

        if (type == REPLICATED) {
            // Replicated distribution assumes that the whole result set is available on all members.
            return true;
        }

        return false;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public RelTraitDef getTraitDef() {
        return traitDef;
    }

    @SuppressWarnings("checkstyle:NPathComplexity")
    @Override
    public boolean satisfies(RelTrait targetTrait) {
        if (!(targetTrait instanceof DistributionTrait)) {
            return false;
        }

        // For single-member deployments all distributions satisfy each other.
        if (traitDef.getMemberCount() == 1) {
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
            for (List<Integer> currentGroup : currentTrait.getFieldGroups()) {
                for (List<Integer> targetGroup : targetTrait.getFieldGroups()) {
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
    private static boolean satisfiesPartitioned(List<Integer> currentFields, List<Integer> targetFields) {
        if (currentFields.size() > targetFields.size()) {
            return false;
        }

        for (Integer currentField : currentFields) {
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

    private static void appendFieldGroupToString(StringBuilder builder, List<Integer> fields) {
        builder.append("{");

        for (int i = 0; i < fields.size(); i++) {
            if (i != 0) {
                builder.append(", ");
            }

            Integer field = fields.get(i);

            builder.append("$").append(field);
        }

        builder.append("}");
    }

    private static String fieldGroupToString(List<Integer> fields) {
        StringBuilder res = new StringBuilder();

        appendFieldGroupToString(res, fields);

        return res.toString();
    }

    /**
     * Field group comparator. Sorts several distribution groups in alphabetical order, so that distribution of
     * {a1, a2} + {b1, b2} is considered equal to {b1, b2} + {a1, a2}.
     */
    static final class FieldGroupComparator implements Comparator<List<Integer>> {
        /** Singleton instance. */
        static final FieldGroupComparator INSTANCE = new FieldGroupComparator();

        @Override
        public int compare(List<Integer> o1, List<Integer> o2) {
            String str1 = fieldGroupToString(o1);
            String str2 = fieldGroupToString(o2);

            return str1.compareTo(str2);
        }
    }
}
