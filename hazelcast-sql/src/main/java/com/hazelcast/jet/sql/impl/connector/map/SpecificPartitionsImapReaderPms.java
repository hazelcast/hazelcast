/*
 * Copyright 2023 Hazelcast Inc.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.PartitioningStrategyUtil;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.connector.HazelcastReaders.LocalMapReaderFunction;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.LocalProcessorMetaSupplier;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.LocalProcessorSupplier;
import com.hazelcast.jet.impl.connector.ReadMapOrCacheP.Reader;
import com.hazelcast.jet.impl.util.FixedCapacityIntArrayList;
import com.hazelcast.partition.Partition;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;

/**
 * A meta-supplier that will compute required partitions to scan,
 * if attribute partitioning strategy was applied.
 */
public abstract class SpecificPartitionsImapReaderPms<F extends CompletableFuture, B, R>
        extends LocalProcessorMetaSupplier<F, B, R> {
    transient int[] partitionsToScan;
    private final List<List<Expression<?>>> requiredPartitionsExprs;
    private transient Map<Address, int[]> partitionAssignment;

    private SpecificPartitionsImapReaderPms(
            final BiFunctionEx<HazelcastInstance, InternalSerializationService, Reader<F, B, R>> readerSupplier,
            @Nullable final List<List<Expression<?>>> requiredPartitionsExprs) {
        super(readerSupplier);
        this.requiredPartitionsExprs = requiredPartitionsExprs;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        super.init(context);
        if (requiredPartitionsExprs != null) {
            FixedCapacityIntArrayList partitionsToScanList = new FixedCapacityIntArrayList(requiredPartitionsExprs.size());

            HazelcastInstance hazelcastInstance = context.hazelcastInstance();
            ExpressionEvalContext eec = ExpressionEvalContext.from(context);
            for (List<Expression<?>> requiredPartitionsExpr : requiredPartitionsExprs) {
                Object[] partitionKeyComponents = new Object[requiredPartitionsExpr.size()];
                int i = 0;
                for (Expression<?> expression : requiredPartitionsExpr) {
                    partitionKeyComponents[i++] = expression.evalTop(null, eec);
                }

                final Partition partition = hazelcastInstance.getPartitionService().getPartition(
                        PartitioningStrategyUtil.constructAttributeBasedKey(partitionKeyComponents)
                );
                if (partition == null) {
                    // Can happen if the cluster is mid-repartitioning/migration, in this case we revert to
                    // non-pruning logic. Alternative scenario is if the produced partitioning key somehow invalid.
                    return;
                }
                assert context.partitionAssignment().values().stream()
                        .anyMatch(pa -> Arrays.binarySearch(pa, partition.getPartitionId()) >= 0)
                        : "Partition calculated for PMS not present in the job";
                partitionsToScanList.add(partition.getPartitionId());
            }
            partitionsToScan = partitionsToScanList.asArray();
            Arrays.sort(partitionsToScan);
            partitionAssignment = context.partitionAssignment();
        }
    }

    @Nonnull
    @Override
    public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        if (partitionsToScan == null) {
            return address -> new LocalProcessorSupplier<>(readerSupplier);
        } else {
            return address -> {
                int[] partitions = partitionAssignment.get(address);
                List<Integer> partitionsToScanList = new ArrayList<>();
                // partitionAssignment is sorted, so we can use binary search
                for (int pId : partitionsToScan) {
                    if (Arrays.binarySearch(partitions, pId) >= 0) {
                        partitionsToScanList.add(pId);
                    }
                }

                int[] memberPartitionsToScan = partitionsToScanList.stream().mapToInt(i -> i).toArray();
                return new LocalProcessorSupplier<>(readerSupplier, memberPartitionsToScan);
            };
        }
    }

    @Override
    public boolean isReusable() {
        return requiredPartitionsExprs == null;
    }

    @Override
    public boolean initIsCooperative() {
        return requiredPartitionsExprs == null ?
                super.initIsCooperative() :
                requiredPartitionsExprs.stream().allMatch(l -> l.stream().allMatch(Expression::isCooperative));
    }

    // TODO: name it properly.
    public static ProcessorMetaSupplier mapReader(String mapName, @Nullable List<List<Expression<?>>> requiredPartitionsExprs) {
        return new SpecificPartitionsImapReaderPms<>(new LocalMapReaderFunction(mapName), requiredPartitionsExprs) {
            @Override
            public Permission getRequiredPermission() {
                return new MapPermission(mapName, ACTION_CREATE, ACTION_READ);
            }
        };
    }
}
