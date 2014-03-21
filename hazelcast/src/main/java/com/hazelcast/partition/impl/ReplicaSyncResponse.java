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

package com.hazelcast.partition.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;

import static com.hazelcast.nio.IOUtil.closeResource;

@edu.umd.cs.findbugs.annotations.SuppressWarnings("EI_EXPOSE_REP")
public class ReplicaSyncResponse extends Operation
        implements PartitionAwareOperation, BackupOperation, UrgentSystemOperation {

    private byte[] data;
    private long[] replicaVersions;
    private boolean compressed;

    public ReplicaSyncResponse() {
    }

    public ReplicaSyncResponse(byte[] data, long[] replicaVersions, boolean compressed) {
        this.data = data;
        this.replicaVersions = replicaVersions;
        this.compressed = compressed;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        SerializationService serializationService = nodeEngine.getSerializationService();
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();
        BufferObjectDataInput in = null;
        try {
            if (data != null && data.length > 0) {
                logApplyReplicaSync(partitionId, replicaIndex);
                byte[] taskData = compressed ? IOUtil.decompress(data) : data;
                in = serializationService.createObjectDataInput(taskData);
                int size = in.readInt();
                for (int i = 0; i < size; i++) {
                    Operation op = (Operation) serializationService.readObject(in);
                    try {
                        ErrorLoggingResponseHandler responseHandler
                                = new ErrorLoggingResponseHandler(nodeEngine.getLogger(op.getClass()));
                        op.setNodeEngine(nodeEngine)
                                .setPartitionId(partitionId)
                                .setReplicaIndex(replicaIndex)
                                .setResponseHandler(responseHandler);
                        op.beforeRun();
                        op.run();
                        op.afterRun();
                    } catch (Throwable e) {
                        logException(op, e);
                    }
                }
            }
        } finally {
            closeResource(in);
            partitionService.finalizeReplicaSync(partitionId, replicaVersions);
        }
    }

    private void logException(Operation op, Throwable e) {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        ILogger logger = nodeEngine.getLogger(getClass());
        Level level = nodeEngine.isActive() ? Level.WARNING : Level.FINEST;
        if (logger.isLoggable(level)) {
            logger.log(level, "While executing " + op, e);
        }
    }

    private void logApplyReplicaSync(int partitionId, int replicaIndex) {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        ILogger logger = nodeEngine.getLogger(getClass());
        if (logger.isFinestEnabled()) {
            logger.finest("Applying replica sync for partition: " + partitionId + ", replica: " + replicaIndex);
        }
    }


    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    public boolean validatesTarget() {
        return true;
    }

    @Override
    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        IOUtil.writeByteArray(out, data);
        out.writeLongArray(replicaVersions);
        out.writeBoolean(compressed);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        data = IOUtil.readByteArray(in);
        replicaVersions = in.readLongArray();
        compressed = in.readBoolean();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ReplicaSyncResponse");
        sb.append("{partition=").append(getPartitionId());
        sb.append(", replica=").append(getReplicaIndex());
        sb.append(", version=").append(Arrays.toString(replicaVersions));
        sb.append('}');
        return sb.toString();
    }

    private static final class ErrorLoggingResponseHandler implements ResponseHandler {
        private final ILogger logger;

        private ErrorLoggingResponseHandler(ILogger logger) {
            this.logger = logger;
        }

        @Override
        public void sendResponse(final Object obj) {
            if (obj instanceof Throwable) {
                Throwable t = (Throwable) obj;
                logger.severe(t);
            }
        }

        @Override
        public boolean isLocal() {
            return true;
        }
    }
}
