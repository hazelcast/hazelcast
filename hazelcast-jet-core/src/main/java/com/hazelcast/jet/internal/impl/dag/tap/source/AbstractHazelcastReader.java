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

package com.hazelcast.jet.internal.impl.dag.tap.source;

import com.hazelcast.jet.internal.api.actor.ProducerCompletionHandler;
import com.hazelcast.jet.internal.impl.util.JetUtil;
import com.hazelcast.jet.internal.impl.util.SettableFuture;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.container.ContainerDescriptor;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.api.data.DataReader;
import com.hazelcast.jet.api.data.tuple.JetTupleFactory;
import com.hazelcast.jet.api.strategy.DataTransferringStrategy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public abstract class AbstractHazelcastReader<V> implements DataReader {
    protected final SettableFuture<Boolean> future = SettableFuture.create();
    protected final NodeEngine nodeEngine;
    protected final JetTupleFactory tupleFactory;
    protected final ContainerDescriptor containerDescriptor;
    protected final ILogger logger;
    protected long position;
    protected Iterator<V> iterator;
    protected volatile Object[] chunkBuffer;

    protected final PartitionSpecificRunnable partitionSpecificOpenRunnable = new PartitionSpecificRunnable() {
        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            try {
                onOpen();
                future.set(true);
            } catch (Throwable e) {
                future.setException(e);
            }
        }
    };

    protected final PartitionSpecificRunnable partitionSpecificCloseRunnable = new PartitionSpecificRunnable() {
        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            try {
                onClose();
                future.set(true);
            } catch (Throwable e) {
                future.setException(e);
            }
        }
    };

    protected final PartitionSpecificRunnable partitionSpecificRunnable = new PartitionSpecificRunnable() {
        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            try {
                readFromCurrentThread();
                future.set(true);
            } catch (Throwable e) {
                future.setException(e);
            }
        }
    };

    private boolean closed;
    private Object[] buffer;
    private final String name;
    private boolean markClosed;
    private final Vertex vertex;
    private final int chunkSize;
    private final int partitionId;
    private final int awaitSecondsTime;
    private volatile int lastProducedCount;
    private final InternalOperationService internalOperationService;
    private final DataTransferringStrategy dataTransferringStrategy;
    private final List<ProducerCompletionHandler> completionHandlers;
    private volatile boolean isReadRequested;

    public AbstractHazelcastReader(ContainerDescriptor containerDescriptor,
                                   String name,
                                   int partitionId,
                                   JetTupleFactory tupleFactory,
                                   Vertex vertex,
                                   DataTransferringStrategy dataTransferringStrategy
    ) {
        this.name = name;
        this.vertex = vertex;
        this.partitionId = partitionId;
        this.tupleFactory = tupleFactory;
        this.containerDescriptor = containerDescriptor;
        this.nodeEngine = containerDescriptor.getNodeEngine();
        this.logger = nodeEngine.getLogger(getClass());
        this.completionHandlers = new CopyOnWriteArrayList<ProducerCompletionHandler>();
        this.internalOperationService = (InternalOperationService) this.nodeEngine.getOperationService();
        JetApplicationConfig config = containerDescriptor.getConfig();
        this.awaitSecondsTime = config.getJetSecondsToAwait();
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
    public boolean hasNext() {
        return this.iterator != null && iterator.hasNext();
    }

    @Override
    public void close() {
        this.partitionSpecificCloseRunnable.run();
    }

    protected abstract void onClose();

    protected abstract void onOpen();

    @Override
    public void open() {
        this.closed = false;
        this.markClosed = false;

        if (readFromPartitionThread()) {
            this.future.reset();
            this.internalOperationService.execute(this.partitionSpecificOpenRunnable);

            try {
                this.future.get(this.awaitSecondsTime, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw JetUtil.reThrow(e);
            }
        } else {
            this.partitionSpecificOpenRunnable.run();
            checkFuture();
        }
    }

    private void checkFuture() {
        try {
            future.get();
        } catch (Throwable e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public int lastProducedCount() {
        return this.lastProducedCount;
    }

    @Override
    public void registerCompletionHandler(ProducerCompletionHandler handler) {
        this.completionHandlers.add(handler);
    }

    @Override
    public boolean isClosed() {
        return this.closed;
    }

    @Override
    public boolean isShuffled() {
        return true;
    }

    private void pushReadRequest() {
        this.isReadRequested = true;
        this.future.reset();
        this.internalOperationService.execute(this.partitionSpecificRunnable);
    }

    @SuppressWarnings("unchecked")
    private Object[] readFromCurrentThread() {
        if (this.markClosed) {
            if (this.closed) {
                return null;
            } else {
                return closeReader();
            }
        }

        if (!hasNext()) {
            return closeReader();
        }

        int idx = 0;

        boolean hashNext;

        do {
            V value = this.iterator.next();

            this.position++;

            if (value != null) {
                if (getDataTransferringStrategy().byReference()) {
                    this.buffer[idx++] = value;
                } else {
                    getDataTransferringStrategy().copy(value, this.buffer[idx++]);
                }
            }
            hashNext = hasNext();
        } while ((hashNext) && (idx < this.chunkSize));

        processBuffers(idx);

        if (!hashNext) {
            this.markClosed = true;
        }

        return this.chunkBuffer;
    }

    private void processBuffers(int idx) {
        if (idx == 0) {
            this.chunkBuffer = null;
            this.lastProducedCount = 0;
        } else if ((idx > 0) && (idx < this.chunkSize)) {
            Object[] trunkedChunk = new Object[idx];
            System.arraycopy(this.buffer, 0, trunkedChunk, 0, idx);
            this.chunkBuffer = trunkedChunk;
            this.lastProducedCount = idx;
        } else {
            this.chunkBuffer = buffer;
            this.lastProducedCount = buffer.length;
        }
    }

    private Object[] closeReader() {
        if (!this.isClosed()) {
            try {
                close();
            } finally {
                handleProducerCompleted();
            }
        }

        this.reset();

        return null;
    }

    private void reset() {
        this.closed = true;
        this.markClosed = true;
        this.chunkBuffer = null;
        this.lastProducedCount = -1;
    }

    private Object[] doReadFromPartitionThread() {
        if (this.isReadRequested) {
            if (this.future.isDone()) {
                try {
                    this.future.get();
                    Object[] chunk = this.chunkBuffer;
                    return chunk;
                } catch (Throwable e) {
                    throw JetUtil.reThrow(e);
                } finally {
                    this.future.reset();
                    this.chunkBuffer = null;
                    this.isReadRequested = false;
                }
            } else {
                return null;
            }
        } else {
            if (isClosed()) {
                return null;
            }

            pushReadRequest();
            return null;
        }
    }

    @Override
    public Object[] read() {
        return readFromCurrentThread();
    }

    @Override
    public Object[] produce() {
        return readFromPartitionThread() ? doReadFromPartitionThread() : readFromCurrentThread();
    }

    @Override
    public long position() {
        return this.position;
    }

    @Override
    public Vertex getVertex() {
        return this.vertex;
    }

    @Override
    public int getPartitionId() {
        return this.partitionId;
    }

    @Override
    public void handleProducerCompleted() {
        for (ProducerCompletionHandler handler : this.completionHandlers) {
            handler.onComplete(this);
        }
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public DataTransferringStrategy getDataTransferringStrategy() {
        return this.dataTransferringStrategy;
    }
}
