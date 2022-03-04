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

import com.hazelcast.cluster.Address;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.QueryPath;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import static com.hazelcast.jet.Traversers.empty;
import static com.hazelcast.jet.Traversers.singleton;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

@SuppressFBWarnings(
        value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
        justification = "the class is never java-serialized"
)
final class JoinByEquiJoinProcessorSupplier implements ProcessorSupplier, DataSerializable {

    private JetJoinInfo joinInfo;
    private String mapName;
    private int partitionCount;
    private List<Integer> partitions;
    private KvRowProjector.Supplier rightRowProjectorSupplier;

    private transient MapProxyImpl<Object, Object> map;
    private transient ExpressionEvalContext evalContext;
    private transient Extractors extractors;

    @SuppressWarnings("unused")
    private JoinByEquiJoinProcessorSupplier() {
    }

    JoinByEquiJoinProcessorSupplier(
            @Nonnull JetJoinInfo joinInfo,
            @Nonnull String mapName,
            int partitionCount,
            @Nullable List<Integer> partitions,
            @Nonnull KvRowProjector.Supplier rightRowProjectorSupplier
    ) {
        assert joinInfo.isEquiJoin() && (joinInfo.isInner() || joinInfo.isLeftOuter());

        this.joinInfo = joinInfo;
        this.mapName = mapName;
        this.partitionCount = partitionCount;
        this.partitions = partitions;
        this.rightRowProjectorSupplier = rightRowProjectorSupplier;
    }

    @Override
    public void init(@Nonnull Context context) {
        map = (MapProxyImpl<Object, Object>) context.hazelcastInstance().getMap(mapName);
        evalContext = ExpressionEvalContext.from(context);
        extractors = Extractors.newBuilder(evalContext.getSerializationService()).build();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            PartitionIdSet partitions = this.partitions == null
                    ? null
                    : new PartitionIdSet(partitionCount, this.partitions);
            QueryPath[] rightPaths = rightRowProjectorSupplier.paths();
            KvRowProjector rightProjector = rightRowProjectorSupplier.get(evalContext, extractors);
            Processor processor = new TransformP<JetSqlRow, JetSqlRow>(
                    joinFn(joinInfo, map, partitions, rightPaths, rightProjector, evalContext)
            ) {
                @Override
                public boolean isCooperative() {
                    return false;
                }
            };
            processors.add(processor);
        }
        return processors;
    }

    private static FunctionEx<JetSqlRow, Traverser<JetSqlRow>> joinFn(
            JetJoinInfo joinInfo,
            MapProxyImpl<Object, Object> map,
            PartitionIdSet partitions,
            QueryPath[] rightPaths,
            KvRowProjector rightRowProjector,
            ExpressionEvalContext evalContext
    ) {
        return left -> {
            Predicate<Object, Object> predicate = QueryUtil.toPredicate(
                    left,
                    joinInfo.leftEquiJoinIndices(),
                    joinInfo.rightEquiJoinIndices(),
                    rightPaths
            );
            if (predicate == null) {
                return joinInfo.isInner()
                        ? empty()
                        : singleton(left.extendedRow(rightRowProjector.getColumnCount()));
            }

            Set<Entry<Object, Object>> matchingRows = joinInfo.isInner()
                    ? map.entrySet(predicate, partitions.copy())
                    : map.entrySet(predicate);
            List<JetSqlRow> joined = join(left, matchingRows, rightRowProjector, joinInfo.nonEquiCondition(), evalContext);
            return joined.isEmpty() && joinInfo.isLeftOuter()
                    ? singleton(left.extendedRow(rightRowProjector.getColumnCount()))
                    : traverseIterable(joined);
        };
    }

    private static List<JetSqlRow> join(
            JetSqlRow left,
            Set<Entry<Object, Object>> entries,
            KvRowProjector rightRowProjector,
            Expression<Boolean> condition,
            ExpressionEvalContext evalContext
    ) {
        List<JetSqlRow> rows = new ArrayList<>();
        for (Entry<Object, Object> entry : entries) {
            JetSqlRow right = rightRowProjector.project(entry.getKey(), entry.getValue());
            if (right == null) {
                continue;
            }

            JetSqlRow joined = ExpressionUtil.join(left, right, condition, evalContext);
            if (joined != null) {
                rows.add(joined);
            }
        }
        return rows;
    }

    @Override
    public List<Permission> permissions() {
        return singletonList(new MapPermission(mapName, ACTION_CREATE, ACTION_READ));
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(joinInfo);
        out.writeObject(mapName);
        out.writeInt(partitionCount);
        out.writeObject(partitions);
        out.writeObject(rightRowProjectorSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        joinInfo = in.readObject();
        mapName = in.readObject();
        partitionCount = in.readInt();
        partitions = in.readObject();
        rightRowProjectorSupplier = in.readObject();
    }

    static ProcessorMetaSupplier supplier(
            JetJoinInfo joinInfo,
            String mapName,
            KvRowProjector.Supplier rightRowProjectorSupplier
    ) {
        return new Supplier(joinInfo, mapName, rightRowProjectorSupplier);
    }

    @SuppressFBWarnings(
            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized"
    )
    private static final class Supplier implements ProcessorMetaSupplier, DataSerializable {

        private JetJoinInfo joinInfo;
        private String mapName;
        private KvRowProjector.Supplier rightRowProjectorSupplier;

        private transient PartitionService partitionService;

        @SuppressWarnings("unused")
        private Supplier() {
        }

        private Supplier(
                JetJoinInfo joinInfo,
                String mapName,
                KvRowProjector.Supplier rightRowProjectorSupplier
        ) {
            assert joinInfo.isEquiJoin() && (joinInfo.isInner() || joinInfo.isLeftOuter());

            this.joinInfo = joinInfo;
            this.mapName = mapName;
            this.rightRowProjectorSupplier = rightRowProjectorSupplier;
        }

        @Override
        public void init(@Nonnull Context context) {
            this.partitionService = context.hazelcastInstance().getPartitionService();
        }

        @Nonnull
        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            if (joinInfo.isInner()) {
                Set<Partition> partitions = partitionService.getPartitions();
                int partitionCount = partitions.size();
                Map<Address, List<Integer>> partitionsByMember = Util.assignPartitions(
                        addresses,
                        partitions.stream()
                                  .collect(groupingBy(
                                          partition -> partition.getOwner().getAddress(),
                                          mapping(Partition::getPartitionId, toList()))
                                  )
                );

                return address -> new JoinByEquiJoinProcessorSupplier(
                        joinInfo,
                        mapName,
                        partitionCount,
                        partitionsByMember.get(address),
                        rightRowProjectorSupplier
                );
            } else {
                return address -> new JoinByEquiJoinProcessorSupplier(
                        joinInfo,
                        mapName,
                        0,
                        null,
                        rightRowProjectorSupplier
                );
            }
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(joinInfo);
            out.writeObject(mapName);
            out.writeObject(rightRowProjectorSupplier);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            joinInfo = in.readObject();
            mapName = in.readObject();
            rightRowProjectorSupplier = in.readObject();
        }
    }
}
