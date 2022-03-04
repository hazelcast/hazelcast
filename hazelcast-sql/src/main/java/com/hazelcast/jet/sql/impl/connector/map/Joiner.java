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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.SqlConnector.VertexWithInputConfig;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.extract.QueryPath;

final class Joiner {

    /**
     * A {@link Data} value that's used instead of a null key.
     */
    private static final Data NULL_KEY_MARKER = new HeapData(new byte[0]);

    private Joiner() {
    }

    static VertexWithInputConfig join(
            DAG dag,
            String mapName,
            String tableName,
            JetJoinInfo joinInfo,
            KvRowProjector.Supplier rightRowProjectorSupplier
    ) {
        int leftEquiJoinPrimitiveKeyIndex = leftEquiJoinPrimitiveKeyIndex(joinInfo, rightRowProjectorSupplier.paths());
        if (leftEquiJoinPrimitiveKeyIndex > -1) {
            // This branch handles the case when there's an equi-join condition for the __key field.
            // For example: SELECT * FROM left [LEFT] JOIN right ON left.field1=right.__key
            // In this case we'll use map.get() for the right map to get the matching entry by key and evaluate the
            // remaining conditions on the returned row.
            return new VertexWithInputConfig(
                    dag.newUniqueVertex(
                            "Join(Lookup-" + tableName + ")",
                            new JoinByPrimitiveKeyProcessorSupplier(
                                    joinInfo.isInner(),
                                    leftEquiJoinPrimitiveKeyIndex,
                                    joinInfo.condition(),
                                    mapName,
                                    rightRowProjectorSupplier
                            )
                    ),
                    edge -> edge.distributed().partitioned(extractPrimitiveKeyFn(leftEquiJoinPrimitiveKeyIndex))
            );
        } else if (joinInfo.isEquiJoin()) {
            // This branch handles the case when there's an equi-join, but not for __key (that was handled above)
            // For example: SELECT * FROM left JOIN right ON left.field1=right.field1
            // In this case we'll construct a com.hazelcast.query.Predicate that will find matching rows using
            // the `map.entrySet(predicate)` method.
            assert joinInfo.isLeftOuter() || joinInfo.isInner();
            return new VertexWithInputConfig(
                    dag.newUniqueVertex(
                            "Join(Predicate-" + tableName + ")",
                            JoinByEquiJoinProcessorSupplier.supplier(joinInfo, mapName, rightRowProjectorSupplier)
                    ),
                    edge -> {
                        // In case of an inner join we'll use `entrySet(predicate, partitionIdSet)` - we'll fan-out each
                        // left item to all members and each member will query a subset of partitions (the local ones).
                        // Otherwise, a default edge is used (local unicast)
                        if (joinInfo.isInner()) {
                            edge.distributed().fanout();
                        }
                    });
        } else {
            // This is the fallback case when there's not an equi-join: it can be a cross-join or join on
            // another condition. For example:
            // SELECT * FROM houses h JOIN renters r WHERE h.rent BETWEEN r.min_rent AND r.max_rent
            return new VertexWithInputConfig(
                    dag.newUniqueVertex(
                            "Join(Scan-" + tableName + ")",
                            new JoinScanProcessorSupplier(joinInfo, mapName, rightRowProjectorSupplier)
                    )
            );
        }
        // TODO: detect and handle always-false condition ?
    }

    /**
     * Find the index of the field of the left side of a join that is in equals
     * predicate with the entire right-side key. Returns -1 if there's no
     * equi-join condition or the equi-join doesn't compare the key object of
     * the right side.
     * <p>
     * For example, in:
     * <pre>{@code
     *     SELECT *
     *     FROM l
     *     JOIN r ON l.field1=right.__key
     * }</pre>
     * it will return the index of {@code field1} in the left table.
     */
    private static int leftEquiJoinPrimitiveKeyIndex(JetJoinInfo joinInfo, QueryPath[] rightPaths) {
        int[] rightEquiJoinIndices = joinInfo.rightEquiJoinIndices();
        for (int i = 0; i < rightEquiJoinIndices.length; i++) {
            QueryPath path = rightPaths[rightEquiJoinIndices[i]];
            if (path.isTop() && path.isKey()) {
                return joinInfo.leftEquiJoinIndices()[i];
            }
        }
        return -1;
    }

    private static FunctionEx<Object, Data> extractPrimitiveKeyFn(int index) {
        return row -> {
            Data value = ((JetSqlRow) row).getSerialized(index);
            return value == null ? NULL_KEY_MARKER : value;
        };
    }
}
