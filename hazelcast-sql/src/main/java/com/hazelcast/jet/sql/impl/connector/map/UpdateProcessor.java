/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.Util.distributeObjects;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

public final class UpdateProcessor extends AbstractProcessor {
    private String mapName;
    private KvRowProjector.Supplier mapEntryProjectorSupplier;
    private int[] updateColumns;

    private MapProxyImpl<Object, Object> map;
    private ExpressionEvalContext evalContext;
    private Extractors extractors;

    @SuppressWarnings("unused")
    private UpdateProcessor() {
    }

    private UpdateProcessor(String mapName, KvRowProjector.Supplier mapEntryProjectorSupplier, int[] updateColumns) {
        this.mapName = mapName;
        this.mapEntryProjectorSupplier = mapEntryProjectorSupplier;
        this.updateColumns = updateColumns;
    }

    @Override
    public void init(@Nonnull Context context) {
        map = (MapProxyImpl<Object, Object>) context.jetInstance().getMap(mapName);
        evalContext = SimpleExpressionEvalContext.from(context);
        extractors = Extractors.newBuilder(evalContext.getSerializationService()).build();
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        int columnCount = mapEntryProjectorSupplier.columnCount();
        KvRowProjector kvRowProjector = mapEntryProjectorSupplier.get(evalContext, extractors);
        for (Object[] row; (row = (Object[]) inbox.peek()) != null; ) {
            inbox.remove();
            Object key = row[0];
            Object value = map.get(key);
            Map.Entry<Object, Object> entry = Util.entry(key, value);
            Object[] project = kvRowProjector.project(entry);
            if (updateColumns.length == 0) {
                // 0 - __key
                // 1 - old this
                // 2 - new this
                project[1] = row[2];
            } else {
                int i = 0;
                for (int index : updateColumns) {
                    project[index] = row[columnCount + i];
                    i++;
                }
            }
            getOutbox().offer(project);
        }
    }

    @SuppressFBWarnings(
            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized"
    )
    private static final class UpdateProcessorMetaSupplier implements ProcessorMetaSupplier, DataSerializable {
        private String mapName;
        private KvRowProjector.Supplier mapEntryProjectorSupplier;
        private int[] updateColumns;

        private transient PartitionService partitionService;

        @SuppressWarnings("unused")
        private UpdateProcessorMetaSupplier() {
        }

        private UpdateProcessorMetaSupplier(
                String mapName,
                KvRowProjector.Supplier mapEntryProjectorSupplier,
                int[] updateColumns) {
            this.mapName = mapName;
            this.mapEntryProjectorSupplier = mapEntryProjectorSupplier;
            this.updateColumns = updateColumns;
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            this.partitionService = context.jetInstance().getHazelcastInstance().getPartitionService();
        }

        @Nonnull
        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            Set<Partition> partitions = partitionService.getPartitions();
            Map<Address, List<Integer>> partitionsByMember = com.hazelcast.jet.impl.util.Util.assignPartitions(
                    addresses,
                    partitions.stream()
                            .collect(groupingBy(
                                    partition -> partition.getOwner().getAddress(),
                                    mapping(Partition::getPartitionId, toList()))
                            )
            );

            return address -> new UpdateProcessor.UpdateProcessorSupplier(
                    mapName,
                    mapEntryProjectorSupplier,
                    updateColumns,
                    partitionsByMember.get(address));
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(mapName);
            out.writeObject(mapEntryProjectorSupplier);
            out.writeIntArray(updateColumns);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            mapName = in.readString();
            mapEntryProjectorSupplier = in.readObject();
            updateColumns = in.readIntArray();
        }
    }

    public static UpdateProcessorMetaSupplier metaSupplier(
            String mapName,
            KvRowProjector.Supplier mapEntryProjectorSupplier,
            int[] updateColumns) {
        return new UpdateProcessorMetaSupplier(mapName, mapEntryProjectorSupplier, updateColumns);
    }

    @SuppressFBWarnings(
            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized"
    )
    private static class UpdateProcessorSupplier implements ProcessorSupplier, DataSerializable {
        private String mapName;
        private KvRowProjector.Supplier mapEntryProjectorSupplier;
        private int[] updateColumns;
        private List<Integer> memberPartitions;

        @SuppressWarnings("unused")
        private UpdateProcessorSupplier() {
        }

        UpdateProcessorSupplier(
                String mapName,
                KvRowProjector.Supplier mapEntryProjectorSupplier,
                int[] updateColumns,
                List<Integer> memberPartitions) {
            this.mapName = mapName;
            this.mapEntryProjectorSupplier = mapEntryProjectorSupplier;
            this.updateColumns = updateColumns;
            this.memberPartitions = memberPartitions;
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return distributeObjects(count, memberPartitions).values().stream()
                    .map(partitions -> new UpdateProcessor(mapName, mapEntryProjectorSupplier, updateColumns))
                    .collect(toList());
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(mapName);
            out.writeObject(mapEntryProjectorSupplier);
            out.writeIntArray(updateColumns);
            out.writeIntArray(memberPartitions.stream().mapToInt(Integer::intValue).toArray());
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            mapName = in.readString();
            mapEntryProjectorSupplier = in.readObject();
            updateColumns = in.readIntArray();
            int[] partitions = in.readIntArray();
            memberPartitions = new ArrayList<>(partitions.length);
            for (int partitionId : partitions) {
                memberPartitions.add(partitionId);
            }
        }
    }
}
