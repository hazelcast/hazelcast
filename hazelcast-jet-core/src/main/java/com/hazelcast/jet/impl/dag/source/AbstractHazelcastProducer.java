/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.dag.source;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.util.SettableFuture;
import com.hazelcast.jet.runtime.Producer;
import com.hazelcast.jet.runtime.ProducerCompletionHandler;
import com.hazelcast.jet.strategy.DataTransferringStrategy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

public abstract class AbstractHazelcastProducer<V> implements Producer {
    protected final NodeEngine nodeEngine;
    protected final ILogger logger;

    private final String name;
    private final int chunkSize;
    private final int partitionId;
    private final Object[] buffer;
    private final SettableFuture<Object[]> future = SettableFuture.create();
    private final PartitionSpecificRunnable readRunnable = new ReadRunnable();
    private final List<ProducerCompletionHandler> completionHandlers = new CopyOnWriteArrayList<>();
    private final InternalOperationService internalOperationService;
    private final DataTransferringStrategy dataTransferringStrategy;
    private volatile Iterator<V> iterator;
    private volatile int lastProducedCount;
    private volatile boolean isReadInProgress;

    protected AbstractHazelcastProducer(JobContext jobContext, String name, int partitionId,
                                        DataTransferringStrategy dataTransferringStrategy
    ) {
        this.name = name;
        this.partitionId = partitionId;
        this.nodeEngine = jobContext.getNodeEngine();
        this.logger = nodeEngine.getLogger(getClass());
        this.internalOperationService = (InternalOperationService) this.nodeEngine.getOperationService();
        this.dataTransferringStrategy = dataTransferringStrategy;
        JobConfig config = jobContext.getJobConfig();
        this.chunkSize = config.getChunkSize();
        this.buffer = new Object[this.chunkSize];
        if (!this.dataTransferringStrategy.byReference()) {
            for (int i = 0; i < this.buffer.length; i++) {
                this.buffer[i] = this.dataTransferringStrategy.newInstance();
            }
        }
    }

    @Override
    public void open() {
        iterator = newIterator();
    }

    @Override
    public int lastProducedCount() {
        return lastProducedCount;
    }

    @Override
    public void registerCompletionHandler(ProducerCompletionHandler handler) {
        completionHandlers.add(handler);
    }

    @Override
    public Object[] produce() {
        return mustRunOnPartitionThread() ? readOnPartitionThread() : read();
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * @return true if reading must happen on a Hazelcast partition thread, false otherwise
     */
    protected abstract boolean mustRunOnPartitionThread();

    protected abstract Iterator<V> newIterator();

    protected int getPartitionId() {
        return partitionId;
    }


    private Object[] read() {
        if (iterator == null) {
            return null;
        }
        if (!iterator.hasNext()) {
            iterator = null;
            lastProducedCount = -1;
            for (ProducerCompletionHandler handler : completionHandlers) {
                handler.onComplete(this);
            }
            return null;
        }
        int idx = 0;
        do {
            V value = iterator.next();
            if (value != null) {
                if (dataTransferringStrategy.byReference()) {
                    buffer[idx] = value;
                } else {
                    dataTransferringStrategy.copy(value, buffer[idx]);
                }
                idx++;
            }
        } while (iterator.hasNext() && idx < chunkSize);
        lastProducedCount = idx;
        return idx == 0 ? null : buffer;
    }

    private Object[] readOnPartitionThread() {
        if (!isReadInProgress) {
            if (iterator != null) {
                isReadInProgress = true;
                internalOperationService.execute(readRunnable);
            }
            return null;
        }
        if (!future.isDone()) {
            return null;
        }
        try {
            return future.get();
        } catch (Throwable e) {
            throw unchecked(e);
        } finally {
            future.reset();
            isReadInProgress = false;
        }
    }


    private class ReadRunnable implements PartitionSpecificRunnable {
        @Override
        public final int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            try {
                future.set(read());
            } catch (Throwable e) {
                future.setException(e);
            }
        }
    }
}
