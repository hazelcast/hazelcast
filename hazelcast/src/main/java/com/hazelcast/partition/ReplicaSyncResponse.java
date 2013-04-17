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
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.logging.Level;

/**
 * @mdogan 4/11/13
 */
public class ReplicaSyncResponse extends Operation implements PartitionAwareOperation, BackupOperation, FireAndForgetOp {

    private byte[] data;
    private long version;

    public ReplicaSyncResponse() {
    }

    public ReplicaSyncResponse(byte[] data, long version) {
        this.data = data;
        this.version = version;
    }

    public void beforeRun() throws Exception {
    }

    public void run() throws Exception {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final PartitionServiceImpl partitionService = (PartitionServiceImpl) nodeEngine.getPartitionService();
        final SerializationService serializationService = nodeEngine.getSerializationService();
        final int partitionId = getPartitionId();
        final int replicaIndex = getReplicaIndex();
        ObjectDataInput in = null;
        try {
            if (data != null) {
                final ILogger logger = nodeEngine.getLogger(getClass());
                if (logger.isLoggable(Level.FINEST)) {
                    logger.log(Level.FINEST, "Applying replica sync for partition: " + partitionId + ", replica: " + replicaIndex);
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
            partitionService.finalizeReplicaSync(partitionId, version);
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
                logger.log(Level.SEVERE, t.getMessage(), t);
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

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeLong(version);
        IOUtil.writeByteArray(out, data);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        version = in.readLong();
        data = IOUtil.readByteArray(in);
    }
}
