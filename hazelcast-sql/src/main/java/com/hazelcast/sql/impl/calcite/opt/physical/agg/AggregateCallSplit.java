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

package com.hazelcast.sql.impl.calcite.opt.physical.agg;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.calcite.operators.HazelcastSqlOperatorTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Performs split of aggregate calls into local and distributed parts.
 */
public final class AggregateCallSplit {
    /** Local aggregate functions. */
    private final List<AggregateCall> localCalls;

    /** Distributed aggregate functions. */
    private final List<AggregateCall> distributedCalls;

    private AggregateCallSplit(List<AggregateCall> localCalls, List<AggregateCall> distributedCalls) {
        this.localCalls = localCalls;
        this.distributedCalls = distributedCalls;
    }

    /**
     * Perform the split.
     *
     * @param groupCount Number of group attributes.
     * @param calls Calls to be split.
     * @return Split calls.
     */
    @SuppressWarnings("checkstyle:MethodLength")
    public static AggregateCallSplit of(RelOptCluster cluster, int groupCount, List<AggregateCall> calls) {
        List<AggregateCall> localCalls = new ArrayList<>();
        List<AggregateCall> distributedCalls = new ArrayList<>();

        int idx = groupCount;

        for (AggregateCall call : calls) {
            if (call.isDistinct()) {
                // TODO: Add support for distinct aggregates
                throw QueryException.error("Distinct aggregates are not supported: " + call);
            }

            SqlKind kind = call.getAggregation().getKind();

            switch (kind) {
                case SUM:
                case MIN:
                case MAX:
                    // Straightforward implementation, e.g. SUM(x) => SUM(SUM(X))
                    localCalls.add(call);

                    distributedCalls.add(AggregateCall.create(
                        call.getAggregation(),
                        false,
                        false,
                        call.ignoreNulls(),
                        Collections.singletonList(idx++),
                        -1,
                        RelCollations.EMPTY,
                        call.getType(),
                        call.getName()
                    ));

                    break;

                case COUNT:
                    // COUNT(x) => SUM(COUNT(x))
                    localCalls.add(call);

                    distributedCalls.add(AggregateCall.create(
                        SqlStdOperatorTable.SUM,
                        false,
                        false,
                        call.ignoreNulls(),
                        Collections.singletonList(idx++),
                        -1,
                        RelCollations.EMPTY,
                        cluster.getTypeFactory().createSqlType(SqlTypeName.ANY),
                        call.getName()
                    ));

                    break;

                case AVG:
                    // AVG(x) => AVG(SUM(X)/COUNT(X))
                    localCalls.add(AggregateCall.create(
                        SqlStdOperatorTable.SUM,
                        false,
                        false,
                        call.ignoreNulls(),
                        call.getArgList(),
                        -1,
                        RelCollations.EMPTY,
                        cluster.getTypeFactory().createSqlType(SqlTypeName.ANY),
                        call.getName()
                    ));

                    localCalls.add(AggregateCall.create(
                        SqlStdOperatorTable.COUNT,
                        false,
                        false,
                        call.ignoreNulls(),
                        call.getArgList(),
                        -1,
                        RelCollations.EMPTY,
                        cluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
                        call.getName()
                    ));

                    int sumIdx = idx++;
                    int countIdx = idx++;

                    List<Integer> args = Arrays.asList(sumIdx, countIdx);

                    distributedCalls.add(AggregateCall.create(
                        HazelcastSqlOperatorTable.DISTRIBUTED_AVG,
                        false,
                        false,
                        call.ignoreNulls(),
                        args,
                        -1,
                        RelCollations.EMPTY,
                        call.getType(),
                        call.getName()
                    ));

                    break;

                default:
                    throw QueryException.error("Unsupported operation: " + kind);
            }
        }

        return new AggregateCallSplit(localCalls, distributedCalls);
    }

    public List<AggregateCall> getLocalCalls() {
        return localCalls;
    }

    public List<AggregateCall> getDistributedCalls() {
        return distributedCalls;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{localCalls=" + localCalls + ", distributedCalls=" + distributedCalls + '}';
    }
}
