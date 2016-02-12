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

package com.hazelcast.jet.impl.dag.tap.sink;


import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.impl.data.io.DefaultObjectIOStream;
import com.hazelcast.jet.impl.strategy.DefaultHashingStrategy;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.impl.util.SettableFuture;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.spi.container.ContainerDescriptor;
import com.hazelcast.jet.spi.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.spi.data.DataWriter;
import com.hazelcast.jet.spi.strategy.HashingStrategy;
import com.hazelcast.jet.spi.strategy.ShufflingStrategy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;

public abstract class AbstractHazelcastWriter implements DataWriter {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractHazelcastWriter.class);

    protected final DefaultObjectIOStream<Object> chunkInputStream;

    protected final SettableFuture<Boolean> future = SettableFuture.create();

    protected final InternalOperationService internalOperationService;

    protected final ContainerDescriptor containerDescriptor;

    protected final DefaultObjectIOStream<Object> chunkBuffer;
    private final String name;
    private final int partitionId;
    private final NodeEngine nodeEngine;
    private final int awaitInSecondsTime;
    private final SinkTapWriteStrategy sinkTapWriteStrategy;
    private final ShufflingStrategy shufflingStrategy;
    private final PartitionSpecificRunnable partitionSpecificRunnable = new PartitionSpecificRunnable() {
        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            try {
                processChunk(chunkInputStream);
                future.set(true);
            } catch (Throwable e) {
                future.setException(e);
            }
        }
    };
    private final PartitionSpecificRunnable partitionSpecificOpenRunnable = new PartitionSpecificRunnable() {
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
    protected volatile boolean isFlushed = true;
    private int lastConsumedCount;
    private boolean isClosed;

    protected AbstractHazelcastWriter(ContainerDescriptor containerDescriptor,
                                      int partitionId,
                                      SinkTapWriteStrategy sinkTapWriteStrategy) {
        checkNotNull(containerDescriptor);

        this.partitionId = partitionId;
        this.name = containerDescriptor.getApplicationName();
        this.sinkTapWriteStrategy = sinkTapWriteStrategy;
        this.nodeEngine = containerDescriptor.getNodeEngine();
        this.containerDescriptor = containerDescriptor;
        JetApplicationConfig jetApplicationConfig = containerDescriptor.getConfig();
        this.awaitInSecondsTime = jetApplicationConfig.getJetSecondsToAwait();
        this.internalOperationService = (InternalOperationService) this.nodeEngine.getOperationService();
        int tupleChunkSize = jetApplicationConfig.getChunkSize();
        this.chunkBuffer = new DefaultObjectIOStream<Object>(new Object[tupleChunkSize]);
        this.chunkInputStream = new DefaultObjectIOStream<Object>(new Object[tupleChunkSize]);
        this.shufflingStrategy = null;
    }

    @Override
    public boolean consume(ProducerInputStream<Object> chunk) throws Exception {
        return consumeChunk(chunk) > 0;
    }

    private void pushWriteRequest() {
        this.future.reset();
        this.internalOperationService.execute(this.partitionSpecificRunnable);
        this.isFlushed = false;
    }

    @Override
    public int consumeChunk(ProducerInputStream<Object> chunk) throws Exception {
        this.chunkInputStream.consumeStream(chunk);
        pushWriteRequest();
        this.lastConsumedCount = chunk.size();
        return chunk.size();
    }

    @Override
    public int consumeObject(Object object) throws Exception {
        this.chunkBuffer.consume(object);
        this.lastConsumedCount = 1;
        return 1;
    }

    @Override
    public int flush() {
        try {
            if (this.chunkBuffer.size() > 0) {
                return consumeChunk(this.chunkBuffer);
            } else {
                return 0;
            }
        } catch (Exception e) {
            throw JetUtil.reThrow(e);
        }
    }

    protected abstract void processChunk(ProducerInputStream<Object> inputStream);

    public String getName() {
        return this.name;
    }

    @Override
    public int getPartitionId() {
        return this.partitionId;
    }

    public NodeEngine getNodeEngine() {
        return this.nodeEngine;
    }

    public void close() {
        if (!isClosed()) {
            try {
                flush();
            } finally {
                this.isClosed = true;
                onClose();
            }
        }
    }

    protected void onOpen() {

    }

    protected void onClose() {

    }

    public void open() {
        this.future.reset();
        this.isFlushed = true;
        this.isClosed = false;

        this.internalOperationService.execute(this.partitionSpecificOpenRunnable);

        try {
            this.future.get(this.awaitInSecondsTime, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public boolean isFlushed() {
        if (this.isFlushed) {
            return true;
        } else {
            try {
                if (this.future.isDone()) {
                    try {
                        future.get();
                        return true;
                    } finally {
                        this.chunkBuffer.reset();
                        this.isFlushed = true;
                        this.chunkInputStream.reset();
                    }
                }

                return false;
            } catch (Exception e) {
                throw JetUtil.reThrow(e);
            }
        }
    }

    @Override
    public int lastConsumedCount() {
        return this.lastConsumedCount;
    }

    @Override
    public boolean isClosed() {
        return this.isClosed;
    }

    @Override
    public boolean isShuffled() {
        return true;
    }

    @Override
    public SinkTapWriteStrategy getSinkTapWriteStrategy() {
        return this.sinkTapWriteStrategy;
    }

    @Override
    public ShufflingStrategy getShufflingStrategy() {
        return this.shufflingStrategy;
    }

    @Override
    public HashingStrategy getHashingStrategy() {
        return DefaultHashingStrategy.INSTANCE;
    }
}
