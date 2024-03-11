/*
 * Copyright 2024 Hazelcast Inc.
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
import com.hazelcast.jet.kafka.connect.impl.ReadKafkaConnectP;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serial;
import java.util.Collection;

/**
 * This class creates Processors via given ProcessorSupplier and assigns them processorOrder
 */
class TaskMaxProcessorSupplier implements ProcessorSupplier {
    @Serial
    private static final long serialVersionUID = 1L;
    private final ReadKafkaConnectProcessorSupplier supplier;
    private final int lastInitiallyActiveProcesorOrder;
    private final int startingProcessorOrder;

    TaskMaxProcessorSupplier(int startingProcessorOrder,
                             int lastInitiallyActiveProcesorOrder,
                             ReadKafkaConnectProcessorSupplier supplier) {
        this.startingProcessorOrder = startingProcessorOrder;
        this.lastInitiallyActiveProcesorOrder = lastInitiallyActiveProcesorOrder;
        this.supplier = supplier;
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
    public Collection<? extends Processor> get(int count) {
        Collection<ReadKafkaConnectP<?>> processors = supplier.get(count);
        int processorOrder = startingProcessorOrder;
        for (ReadKafkaConnectP<?> processor : processors) {
            int thisOrder = processorOrder++;
            processor.setActive(thisOrder <= lastInitiallyActiveProcesorOrder);
            processor.setProcessorOrder(thisOrder);
        }
        return processors;
    }
}
