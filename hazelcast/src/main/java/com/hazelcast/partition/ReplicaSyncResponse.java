/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.partition;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;

/**
 * @author mdogan 4/11/13
 */
public class ReplicaSyncResponse extends Operation implements PartitionAwareOperation, BackupOperation {

    private byte[] data;
    private long[] replicaVersions;

    public ReplicaSyncResponse() {
    }

    public ReplicaSyncResponse(byte[] data, long[] replicaVersions) {
        this.data = data;
        this.replicaVersions = replicaVersions;
    }

    public void beforeRun() throws Exception {
    }

    public void run() throws Exception {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final PartitionServiceImpl partitionService = (PartitionServiceImpl) nodeEngine.getPartitionService();
        final SerializationService serializationService = nodeEngine.getSerializationService();
        final int partitionId = getPartitionId();
        final int replicaIndex = getReplicaIndex();
        BufferObjectDataInput in = null;
        try {
            if (data != null) {
                final ILogger logger = nodeEngine.getLogger(getClass());
                if (logger.isFinestEnabled()) {
                    logger.finest( "Applying replica sync for partition: " + partitionId + ", replica: " + replicaIndex);
                }
                final byte[] taskData = IOUtil.decompress(data);
                in = serializationService.createObjectDataInput(taskData);
                int size = in.readInt();
                for (int i = 0; i < size; i++) {
                    Operation op = (Operation) serializationService.readObject(in);
                    try {
                        op.setNodeEngine(nodeEngine)
                                .setPartitionId(partitionId).setReplicaIndex(replicaIndex);
                        op.setResponseHandler(new ErrorLoggingResponseHandler(nodeEngine.getLogger(op.getClass())));
                        op.beforeRun();
                        op.run();
                        op.afterRun();
                    } catch (Throwable e) {
                        final Level level = nodeEngine.isActive() ? Level.WARNING : Level.FINEST;
                        logger.log(level, "While executing " + op, e);
                    }
                }
            }
        } finally {
            IOUtil.closeResource(in);
            partitionService.finalizeReplicaSync(partitionId, replicaVersions);
        }
    }

    private class ErrorLoggingResponseHandler implements ResponseHandler {
        private final ILogger logger;

        private ErrorLoggingResponseHandler(ILogger logger) {
            this.logger = logger;
        }

        public void sendResponse(final Object obj) {
            if (obj instanceof Throwable) {
                Throwable t = (Throwable) obj;
                logger.severe(t);
            }
        }
    }

    public void afterRun() throws Exception {
    }

    public boolean returnsResponse() {
        return false;
    }

    public Object getResponse() {
        return null;
    }

    public boolean validatesTarget() {
        return true;
    }

    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        IOUtil.writeByteArray(out, data);
        out.writeLongArray(replicaVersions);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        data = IOUtil.readByteArray(in);
        replicaVersions = in.readLongArray();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("ReplicaSyncResponse");
        sb.append("{partition=").append(getPartitionId());
        sb.append(", replica=").append(getReplicaIndex());
        sb.append(", version=").append(Arrays.toString(replicaVersions));
        sb.append('}');
        return sb.toString();
    }
}
