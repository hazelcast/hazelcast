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


package com.hazelcast.jet.kafka.connect.impl.processorsupplier;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.processor.ExpectNothingP;
import com.hazelcast.jet.kafka.connect.impl.ReadKafkaConnectP;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;

/**
 * This class creates Processors via given ProcessorSupplier and assigns them processorOrder
 */
class TaskMaxProcessorSupplier implements ProcessorSupplier {
    private static final long serialVersionUID = 1L;
    private final int localParallelismForMember;
    private final ReadKafkaConnectProcessorSupplier supplier;
    private int processorOrder;

    TaskMaxProcessorSupplier(int localParallelismForMember,
                                    ReadKafkaConnectProcessorSupplier supplier,
                                    int processorOrder) {
        this.localParallelismForMember = localParallelismForMember;
        this.supplier = supplier;
        this.processorOrder = processorOrder;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        supplier.init(context);
    }

    @Override
    public boolean initIsCooperative() {
        return supplier.initIsCooperative();
    }

    @Override
    public boolean closeIsCooperative() {
        return supplier.closeIsCooperative();
    }

    @Override
    public void close(@Nullable Throwable error) throws Exception {
        supplier.close(error);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int localParallelism) {
        Collection<ReadKafkaConnectP<?>> kafkaConnectProcessorList = getReadKafkaConnectProcessors();

        Collection<Processor> resultList = new ArrayList<>(kafkaConnectProcessorList);
        for (int index = kafkaConnectProcessorList.size(); index < localParallelism; index++) {
            resultList.add(new ExpectNothingP());
        }
        return resultList;
    }

    private Collection<ReadKafkaConnectP<?>> getReadKafkaConnectProcessors() {
        Collection<ReadKafkaConnectP<?>> kafkaProcessors = supplier.get(localParallelismForMember);
        for (ReadKafkaConnectP<?> processor : kafkaProcessors) {
            processor.setProcessorOrder(processorOrder++);
        }
        return kafkaProcessors;
    }
}
