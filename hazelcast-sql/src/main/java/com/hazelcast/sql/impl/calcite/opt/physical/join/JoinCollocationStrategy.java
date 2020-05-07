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

package com.hazelcast.sql.impl.calcite.opt.physical.join;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.sql.impl.calcite.opt.physical.join.JoinCollocationAction.BROADCAST;
import static com.hazelcast.sql.impl.calcite.opt.physical.join.JoinCollocationAction.NONE;
import static com.hazelcast.sql.impl.calcite.opt.physical.join.JoinCollocationAction.REPLICATED_HASH;
import static com.hazelcast.sql.impl.calcite.opt.physical.join.JoinCollocationAction.UNICAST;
import static com.hazelcast.sql.impl.calcite.opt.physical.join.JoinConditionType.EQUI;
import static com.hazelcast.sql.impl.calcite.opt.physical.join.JoinConditionType.OTHER;
import static com.hazelcast.sql.impl.calcite.opt.physical.join.JoinDistribution.PARTITIONED;
import static com.hazelcast.sql.impl.calcite.opt.physical.join.JoinDistribution.RANDOM;
import static com.hazelcast.sql.impl.calcite.opt.physical.join.JoinDistribution.REPLICATED;
import static com.hazelcast.sql.impl.calcite.opt.physical.join.JoinType.FULL;
import static com.hazelcast.sql.impl.calcite.opt.physical.join.JoinType.INNER;
import static com.hazelcast.sql.impl.calcite.opt.physical.join.JoinType.OUTER;
import static com.hazelcast.sql.impl.calcite.opt.physical.join.JoinType.SEMI;

/**
 * Join strategy which described which actions should be performed on inputs to achieve collocation.
 */
public final class JoinCollocationStrategy {
    /** Join strategies. */
    private static final Map<Key, List<JoinCollocationStrategy>> MAP = new HashMap<>();

    /** Action to be performed on the left input. */
    private final JoinCollocationAction leftAction;

    /** Action to be performed on the right input. */
    private final JoinCollocationAction rightAction;

    /** Result distribution. */
    private final JoinDistribution resultDistribution;

    static {
        prepareStrategies();
    }

    private JoinCollocationStrategy(
        JoinCollocationAction leftAction,
        JoinCollocationAction rightAction,
        JoinDistribution resultDistribution
    ) {
        this.leftAction = leftAction;
        this.rightAction = rightAction;
        this.resultDistribution = resultDistribution;
    }

    private static void prepareStrategies() {
        prepareStrategiesInner();
        prepareStrategiesOuter();
        prepareStrategiesFull();
        prepareStrategiesSemi();
    }

    private static void prepareStrategiesInner() {
        addStrategy(INNER, EQUI, REPLICATED, REPLICATED, collocatedPair(REPLICATED));
        addStrategy(INNER, EQUI, REPLICATED, PARTITIONED, collocatedPair(PARTITIONED));
        addStrategy(INNER, EQUI, REPLICATED, RANDOM, collocatedPair(RANDOM));

        addStrategy(INNER, EQUI, PARTITIONED, REPLICATED, collocatedPair(PARTITIONED));
        addStrategy(INNER, EQUI, PARTITIONED, PARTITIONED, collocatedPair(PARTITIONED));
        addStrategy(INNER, EQUI, PARTITIONED, RANDOM, pair(BROADCAST, NONE, RANDOM), pair(NONE, UNICAST, PARTITIONED));

        addStrategy(INNER, EQUI, RANDOM, REPLICATED, collocatedPair(RANDOM));
        addStrategy(INNER, EQUI, RANDOM, PARTITIONED, pair(UNICAST, NONE, PARTITIONED), pair(NONE, BROADCAST, RANDOM));
        addStrategy(INNER, EQUI, RANDOM, RANDOM, pair(UNICAST, UNICAST, PARTITIONED));

        addStrategy(INNER, OTHER, REPLICATED, REPLICATED, collocatedPair(REPLICATED));
        addStrategy(INNER, OTHER, REPLICATED, RANDOM, collocatedPair(RANDOM));

        addStrategy(INNER, OTHER, RANDOM, REPLICATED, collocatedPair(RANDOM));
        addStrategy(INNER, OTHER, RANDOM, RANDOM, pair(BROADCAST, NONE, RANDOM), pair(NONE, BROADCAST, RANDOM));
    }

    private static void prepareStrategiesOuter() {
        addStrategy(OUTER, EQUI, REPLICATED, REPLICATED, collocatedPair(REPLICATED));
        addStrategy(OUTER, EQUI, REPLICATED, PARTITIONED, pair(REPLICATED_HASH, NONE, PARTITIONED));
        addStrategy(OUTER, EQUI, REPLICATED, RANDOM, pair(REPLICATED_HASH, UNICAST, PARTITIONED));

        addStrategy(OUTER, EQUI, PARTITIONED, REPLICATED, collocatedPair(PARTITIONED));
        addStrategy(OUTER, EQUI, PARTITIONED, PARTITIONED, collocatedPair(PARTITIONED));
        addStrategy(OUTER, EQUI, PARTITIONED, RANDOM, pair(NONE, UNICAST, PARTITIONED));

        addStrategy(OUTER, EQUI, RANDOM, REPLICATED, collocatedPair(RANDOM));
        addStrategy(OUTER, EQUI, RANDOM, PARTITIONED, pair(UNICAST, NONE, PARTITIONED), pair(NONE, BROADCAST, RANDOM));
        addStrategy(OUTER, EQUI, RANDOM, RANDOM, pair(UNICAST, UNICAST, PARTITIONED));

        addStrategy(OUTER, OTHER, REPLICATED, REPLICATED, collocatedPair(REPLICATED));
        addStrategy(OUTER, OTHER, REPLICATED, RANDOM, pair(NONE, BROADCAST, REPLICATED));

        addStrategy(OUTER, OTHER, RANDOM, REPLICATED, collocatedPair(RANDOM));
        addStrategy(OUTER, OTHER, RANDOM, RANDOM, pair(NONE, BROADCAST, RANDOM));
    }

    private static void prepareStrategiesFull() {
        addStrategy(FULL, EQUI, REPLICATED, REPLICATED, collocatedPair(REPLICATED));
        addStrategy(FULL, EQUI, REPLICATED, PARTITIONED, pair(REPLICATED_HASH, NONE, PARTITIONED));
        addStrategy(FULL, EQUI, REPLICATED, RANDOM, pair(REPLICATED_HASH, UNICAST, PARTITIONED));

        addStrategy(FULL, EQUI, PARTITIONED, REPLICATED, pair(NONE, REPLICATED_HASH, PARTITIONED));
        addStrategy(FULL, EQUI, PARTITIONED, PARTITIONED, collocatedPair(PARTITIONED));
        addStrategy(FULL, EQUI, PARTITIONED, RANDOM, pair(NONE, UNICAST, PARTITIONED));

        addStrategy(FULL, EQUI, RANDOM, REPLICATED, pair(UNICAST, REPLICATED_HASH, PARTITIONED));
        addStrategy(FULL, EQUI, RANDOM, PARTITIONED, pair(UNICAST, NONE, PARTITIONED));
        addStrategy(FULL, EQUI, RANDOM, RANDOM, pair(UNICAST, UNICAST, PARTITIONED));

        addStrategy(FULL, OTHER, REPLICATED, REPLICATED, collocatedPair(REPLICATED));
        addStrategy(FULL, OTHER, REPLICATED, RANDOM, pair(NONE, BROADCAST, REPLICATED));

        addStrategy(FULL, OTHER, RANDOM, REPLICATED, pair(BROADCAST, NONE, REPLICATED));
        addStrategy(FULL, OTHER, RANDOM, RANDOM, pair(BROADCAST, BROADCAST, REPLICATED));
    }

    private static void prepareStrategiesSemi() {
        addStrategy(SEMI, EQUI, REPLICATED, REPLICATED, collocatedPair(REPLICATED));
        addStrategy(SEMI, EQUI, REPLICATED, PARTITIONED, pair(REPLICATED_HASH, NONE, PARTITIONED));
        addStrategy(SEMI, EQUI, REPLICATED, RANDOM, pair(REPLICATED_HASH, UNICAST, PARTITIONED));

        addStrategy(SEMI, EQUI, PARTITIONED, REPLICATED, collocatedPair(PARTITIONED));
        addStrategy(SEMI, EQUI, PARTITIONED, PARTITIONED, collocatedPair(PARTITIONED));
        addStrategy(SEMI, EQUI, PARTITIONED, RANDOM, pair(NONE, UNICAST, PARTITIONED));

        addStrategy(SEMI, EQUI, RANDOM, REPLICATED, collocatedPair(RANDOM));
        addStrategy(SEMI, EQUI, RANDOM, PARTITIONED, pair(UNICAST, NONE, PARTITIONED), pair(NONE, BROADCAST, RANDOM));
        addStrategy(SEMI, EQUI, RANDOM, RANDOM, pair(UNICAST, UNICAST, PARTITIONED));

        // TODO: Need to optimize ANIT-case for semi joins, since they may behave the same way as equi joins! But the optimizer
        //  is not ready for that yet, and not optimal plans are produced.
        addStrategy(SEMI, OTHER, REPLICATED, REPLICATED, collocatedPair(REPLICATED));
        addStrategy(SEMI, OTHER, REPLICATED, RANDOM, pair(NONE, BROADCAST, REPLICATED));

        addStrategy(SEMI, OTHER, RANDOM, REPLICATED, collocatedPair(RANDOM));
        addStrategy(SEMI, OTHER, RANDOM, RANDOM, pair(NONE, BROADCAST, RANDOM));
    }

    private static void addStrategy(JoinType type,
        JoinConditionType conditionType,
        JoinDistribution leftDistribution,
        JoinDistribution rightDistribution,
        ActionTuple tuple,
        ActionTuple... tuples
    ) {
        int len = 1 + (tuples != null ? tuples.length : 0);

        Key key = Key.of(type, conditionType, leftDistribution, rightDistribution);

        List<JoinCollocationStrategy> strategies = MAP.computeIfAbsent(key, k -> new ArrayList<>(len));

        strategies.add(new JoinCollocationStrategy(tuple.getLeft(), tuple.getRight(), tuple.getResultDistribution()));

        if (tuples != null) {
            for (ActionTuple tuple0 : tuples) {
                strategies.add(new JoinCollocationStrategy(tuple0.getLeft(), tuple0.getRight(), tuple0.getResultDistribution()));
            }
        }
    }

    /**
     * Resolve join collocation strategy for the given type of join, type of condition and input collocations.
     *
     * @param type Join type.
     * @param conditionType Condition type.
     * @param leftDistribution Left distribution.
     * @param rightDistribution Right distribution.
     * @return Strategy.
     */
    public static List<JoinCollocationStrategy> resolve(
        int memberCount,
        JoinType type,
        JoinConditionType conditionType,
        JoinDistribution leftDistribution,
        JoinDistribution rightDistribution
    ) {
        if (memberCount == 1) {
            return Collections.singletonList(new JoinCollocationStrategy(NONE, NONE, leftDistribution));
        }

        Key key = Key.of(type, conditionType, leftDistribution, rightDistribution);

        List<JoinCollocationStrategy> res = MAP.get(key);

        assert res != null : "Missing strategy: " + type + ", " + conditionType + ", " + leftDistribution
            + ", " + rightDistribution;

        return res;
    }

    public JoinCollocationAction getLeftAction() {
        return leftAction;
    }

    public JoinCollocationAction getRightAction() {
        return rightAction;
    }

    public JoinDistribution getResultDistribution() {
        return resultDistribution;
    }

    public boolean isCollocated() {
        return leftAction == NONE && rightAction == NONE;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{leftAction=" + leftAction + ", rightAction=" + rightAction
            + ", result=" + resultDistribution + '}';
    }

    /**
     * Special tuple to denote already collocated mode with no data movement.
     *
     * @param resultDistribution Result distribution.
     * @return Tuple.
     */
    private static ActionTuple collocatedPair(JoinDistribution resultDistribution) {
        return pair(NONE, NONE, resultDistribution);
    }

    private static ActionTuple pair(
        JoinCollocationAction leftAction,
        JoinCollocationAction rightAction,
        JoinDistribution resultDistribution
    ) {
        return new ActionTuple(leftAction, rightAction, resultDistribution);
    }

    private static final class Key {
        private final JoinType type;
        private final JoinConditionType conditionType;
        private final JoinDistribution leftDistribution;
        private final JoinDistribution rightDistribution;

        private Key(
            JoinType type,
            JoinConditionType conditionType,
            JoinDistribution leftDistribution,
            JoinDistribution rightDistribution
        ) {
            this.type = type;
            this.conditionType = conditionType;
            this.leftDistribution = leftDistribution;
            this.rightDistribution = rightDistribution;
        }

        private static Key of(
            JoinType type,
            JoinConditionType conditionType,
            JoinDistribution leftDistribution,
            JoinDistribution rightDistribution
        ) {
            return new Key(type, conditionType, leftDistribution, rightDistribution);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, conditionType, leftDistribution, rightDistribution);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Key key = (Key) o;

            return type == key.type
                && conditionType == key.conditionType
                && leftDistribution == key.leftDistribution
                && rightDistribution == key.rightDistribution;
        }
    }

    /**
     * A pair of actions to be applied to left and right inputs respectively.
     */
    private static final class ActionTuple {
        private final JoinCollocationAction left;
        private final JoinCollocationAction right;
        private final JoinDistribution resultDistribution;

        private ActionTuple(JoinCollocationAction left, JoinCollocationAction right, JoinDistribution resultDistribution) {
            this.left = left;
            this.right = right;
            this.resultDistribution = resultDistribution;
        }

        private JoinCollocationAction getLeft() {
            return left;
        }

        private JoinCollocationAction getRight() {
            return right;
        }

        public JoinDistribution getResultDistribution() {
            return resultDistribution;
        }
    }
}
