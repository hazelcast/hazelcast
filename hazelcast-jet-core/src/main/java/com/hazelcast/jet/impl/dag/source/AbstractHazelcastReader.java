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
import com.hazelcast.jet.impl.actor.ProducerCompletionHandler;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.util.SettableFuture;
import com.hazelcast.jet.runtime.Producer;
import com.hazelcast.jet.strategy.DataTransferringStrategy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

public abstract class AbstractHazelcastReader<V> implements Producer {
    protected final SettableFuture<Boolean> future = SettableFuture.create();
    protected final NodeEngine nodeEngine;
    protected final ILogger logger;
    protected final PartitionSpecificRunnable openRunnable = new ReaderRunnable(this::onOpen);
    protected final PartitionSpecificRunnable closeRunnable = new ReaderRunnable(this::onClose);
    protected final PartitionSpecificRunnable readRunnable = new ReaderRunnable(this::read);

    protected long position;
    protected Iterator<V> iterator;
    protected volatile Object[] chunkBuffer;

    private boolean closed;
    private Object[] buffer;
    private final String name;
    private boolean markClosed;
    private final int chunkSize;
    private final int partitionId;
    private final int awaitSecondsTime;
    private volatile int lastProducedCount;
    private final InternalOperationService internalOperationService;
    private final DataTransferringStrategy dataTransferringStrategy;
    private final List<ProducerCompletionHandler> completionHandlers;
    private volatile boolean isReadRequested;

    protected AbstractHazelcastReader(
            JobContext jobContext, String name, int partitionId,
            DataTransferringStrategy dataTransferringStrategy
    ) {
        this.name = name;
        this.partitionId = partitionId;
        this.nodeEngine = jobContext.getNodeEngine();
        this.logger = nodeEngine.getLogger(getClass());
        this.completionHandlers = new CopyOnWriteArrayList<>();
        this.internalOperationService = (InternalOperationService) this.nodeEngine.getOperationService();
        JobConfig config = jobContext.getJobConfig();
        this.awaitSecondsTime = config.getSecondsToAwait();
        this.chunkSize = config.getChunkSize();
        this.buffer = new Object[this.chunkSize];
        this.dataTransferringStrategy = dataTransferringStrategy;
        if (!this.dataTransferringStrategy.byReference()) {
            for (int i = 0; i < this.buffer.length; i++) {
                this.buffer[i] = this.dataTransferringStrategy.newInstance();
            }
        }
    }

    @Override
    public void close() {
        closeRunnable.run();
    }

    @Override
    public void open() {
        closed = false;
        markClosed = false;
        if (mustRunOnPartitionThread()) {
            future.reset();
            internalOperationService.execute(openRunnable);
            try {
                future.get(awaitSecondsTime, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw unchecked(e);
            }
        } else {
            openRunnable.run();
            awaitFuture();
        }
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
    public boolean isClosed() {
        return closed;
    }

    @Override
    public Object[] produce() {
        return mustRunOnPartitionThread() ? readOnPartitionThread() : read();
    }

    @Override
    public void handleProducerCompleted() {
        for (ProducerCompletionHandler handler : completionHandlers) {
            handler.onComplete(this);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    /** @return true if reading must happen on a Hazelcast partition thread, false otherwise */
    protected abstract boolean mustRunOnPartitionThread();

    protected void onClose() {
    }

    protected void onOpen() {
    }

    protected int getPartitionId() {
        return partitionId;
    }


    private boolean hasNext() {
        return iterator != null && iterator.hasNext();
    }

    private void awaitFuture() {
        try {
            future.get();
        } catch (Throwable e) {
            throw unchecked(e);
        }
    }

    private void pushReadRequest() {
        isReadRequested = true;
        future.reset();
        internalOperationService.execute(readRunnable);
    }

    private Object[] read() {
        if (markClosed) {
            return closed ? null : closeReader();
        }
        if (!hasNext()) {
            return closeReader();
        }
        int idx = 0;
        boolean hashNext;
        do {
            V value = iterator.next();
            position++;
            if (value != null) {
                if (dataTransferringStrategy.byReference()) {
                    buffer[idx++] = value;
                } else {
                    dataTransferringStrategy.copy(value, buffer[idx++]);
                }
            }
            hashNext = hasNext();
        } while ((hashNext) && (idx < chunkSize));
        processBuffers(idx);
        if (!hashNext) {
            markClosed = true;
        }
        return chunkBuffer;
    }

    private void processBuffers(int idx) {
        if (idx == 0) {
            chunkBuffer = null;
            lastProducedCount = 0;
        } else if ((idx > 0) && (idx < chunkSize)) {
            Object[] trunkedChunk = new Object[idx];
            System.arraycopy(buffer, 0, trunkedChunk, 0, idx);
            chunkBuffer = trunkedChunk;
            lastProducedCount = idx;
        } else {
            chunkBuffer = buffer;
            lastProducedCount = buffer.length;
        }
    }

    private Object[] closeReader() {
        if (!isClosed()) {
            try {
                close();
            } finally {
                handleProducerCompleted();
            }
        }
        reset();
        return null;
    }

    private void reset() {
        this.closed = true;
        this.markClosed = true;
        this.chunkBuffer = null;
        this.lastProducedCount = -1;
    }

    private Object[] readOnPartitionThread() {
        if (!isReadRequested) {
            if (!isClosed()) {
                pushReadRequest();
            }
            return null;
        }
        if (!future.isDone()) {
            return null;
        }
        try {
            future.get();
            return chunkBuffer;
        } catch (Throwable e) {
            throw unchecked(e);
        } finally {
            future.reset();
            chunkBuffer = null;
            isReadRequested = false;
        }
    }


    private class ReaderRunnable implements PartitionSpecificRunnable {
        private final Runnable task;

        ReaderRunnable(Runnable task) {
            this.task = task;
        }

        @Override
        public final int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            try {
                task.run();
                future.set(true);
            } catch (Throwable e) {
                future.setException(e);
            }
        }
    }
}
