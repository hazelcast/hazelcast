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

package com.hazelcast.jet.impl.dag.sink;


import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.data.io.IOBuffer;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.strategy.SerializedHashingStrategy;
import com.hazelcast.jet.impl.util.SettableFuture;
import com.hazelcast.jet.runtime.DataWriter;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.strategy.HashingStrategy;
import com.hazelcast.jet.strategy.MemberDistributionStrategy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;
import static com.hazelcast.util.Preconditions.checkNotNull;

public abstract class AbstractHazelcastWriter implements DataWriter {
    protected final IOBuffer<Object> outputBuffer;

    protected final SettableFuture<Boolean> future = SettableFuture.create();

    protected final InternalOperationService internalOperationService;

    protected final IOBuffer<Object> chunkBuffer;

    protected final ILogger logger;

    protected volatile boolean isFlushed = true;

    private final int partitionId;
    private final NodeEngine nodeEngine;
    private final int awaitInSecondsTime;

    private final MemberDistributionStrategy memberDistributionStrategy;
    private final PartitionSpecificRunnable partitionSpecificRunnable = new PartitionSpecificRunnable() {
        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            try {
                processChunk(outputBuffer);
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
    private int lastConsumedCount;
    private boolean isClosed;

    protected AbstractHazelcastWriter(JobContext jobContext, int partitionId) {
        checkNotNull(jobContext);
        this.partitionId = partitionId;
        this.nodeEngine = jobContext.getNodeEngine();
        this.logger = nodeEngine.getLogger(getClass());
        JobConfig jobConfig = jobContext.getJobConfig();
        this.awaitInSecondsTime = jobConfig.getSecondsToAwait();
        this.internalOperationService = (InternalOperationService) this.nodeEngine.getOperationService();
        int pairChunkSize = jobConfig.getChunkSize();
        this.chunkBuffer = new IOBuffer<>(new Object[pairChunkSize]);
        this.outputBuffer = new IOBuffer<>(new Object[pairChunkSize]);
        this.memberDistributionStrategy = null;
    }

    private void pushWriteRequest() {
        this.future.reset();
        this.internalOperationService.execute(this.partitionSpecificRunnable);
        this.isFlushed = false;
    }

    @Override
    public int consume(InputChunk<Object> chunk) {
        this.outputBuffer.collect(chunk);
        pushWriteRequest();
        this.lastConsumedCount = chunk.size();
        return chunk.size();
    }

    @Override
    public int consume(Object object) {
        this.chunkBuffer.collect(object);
        this.lastConsumedCount = 1;
        return 1;
    }

    @Override
    public int flush() {
        try {
            return chunkBuffer.size() > 0 ? consume(chunkBuffer) : 0;
        } catch (Exception e) {
            throw unchecked(e);
        }
    }

    protected abstract void processChunk(InputChunk<Object> inputChunk);

    @Override
    public int getPartitionId() {
        return this.partitionId;
    }

    public NodeEngine getNodeEngine() {
        return this.nodeEngine;
    }

    @Override
    public void close() {
        if (!isClosed) {
            try {
                flush();
            } finally {
                isClosed = true;
                onClose();
            }
        }
    }

    protected void onOpen() {

    }

    protected void onClose() {

    }

    @Override
    public void open() {
        this.future.reset();
        this.isFlushed = true;
        this.isClosed = false;

        this.internalOperationService.execute(this.partitionSpecificOpenRunnable);

        try {
            this.future.get(this.awaitInSecondsTime, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw unchecked(e);
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
                        this.outputBuffer.reset();
                    }
                }

                return false;
            } catch (Exception e) {
                throw unchecked(e);
            }
        }
    }

    @Override
    public int lastConsumedCount() {
        return this.lastConsumedCount;
    }

    @Override
    public boolean isShuffled() {
        return true;
    }

    public MemberDistributionStrategy getMemberDistributionStrategy() {
        return this.memberDistributionStrategy;
    }

    @Override
    public HashingStrategy getHashingStrategy() {
        return SerializedHashingStrategy.INSTANCE;
    }


}
