/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.internal.metrics.impl.MetricsCompressor;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.JobExecutionService;
import com.hazelcast.jet.impl.JobMetricsUtil;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.metrics.RawJobMetrics;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Objects;
import java.util.function.UnaryOperator;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isRestartableException;
import static com.hazelcast.spi.impl.operationservice.ExceptionAction.THROW_EXCEPTION;

public class CompleteExecutionOperation extends Operation implements IdentifiedDataSerializable {

    private long executionId;
    private boolean collectMetrics;
    private Throwable error;
    private RawJobMetrics response;

    public CompleteExecutionOperation() {
    }

    public CompleteExecutionOperation(long executionId, boolean collectMetrics, Throwable error) {
        this.executionId = executionId;
        this.collectMetrics = collectMetrics;
        this.error = error;
    }

    @Override
    public void run() {
        ILogger logger = getLogger();
        JetService service = getService();

        Address callerAddress = getCallerAddress();
        logger.fine("Completing execution " + idToString(executionId) + " from caller " + callerAddress
                + ", error=" + error);

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        Address masterAddress = getNodeEngine().getMasterAddress();
        if (!callerAddress.equals(masterAddress)) {
            throw new IllegalStateException("Caller " + callerAddress + " cannot complete execution "
                    + idToString(executionId) + " because it is not master. Master is: " + masterAddress);
        }

        JobExecutionService jobExecutionService = service.getJobExecutionService();
        jobExecutionService.beforeCompleteExecution(executionId);
        if (collectMetrics) {
            JobMetricsCollector metricsRenderer = new JobMetricsCollector(executionId, nodeEngine.getLocalMember(),
                    logger);
            nodeEngine.getMetricsRegistry().collect(metricsRenderer);
            response = metricsRenderer.getMetrics();
        } else {
            response = RawJobMetrics.empty();
        }

        jobExecutionService.completeExecution(executionId, error);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        return isRestartableException(throwable) ? THROW_EXCEPTION : super.onInvocationException(throwable);
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.COMPLETE_EXECUTION_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(executionId);
        out.writeBoolean(collectMetrics);
        out.writeObject(error);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        executionId = in.readLong();
        collectMetrics = in.readBoolean();
        error = in.readObject();
    }

    private static class JobMetricsCollector implements MetricsCollector {

        private final Long executionIdOfInterest;
        private final MetricsCompressor compressor;
        private final ILogger logger;
        private final UnaryOperator<MetricDescriptor> addPrefixFn;

        JobMetricsCollector(long executionId, @Nonnull Member member, @Nonnull ILogger logger) {
            Objects.requireNonNull(member, "member");
            this.logger = Objects.requireNonNull(logger, "logger");

            this.executionIdOfInterest = executionId;
            this.addPrefixFn = JobMetricsUtil.addMemberPrefixFn(member);
            this.compressor = new MetricsCompressor();
        }

        @Override
        public void collectLong(MetricDescriptor descriptor, long value) {
            Long executionId = JobMetricsUtil.getExecutionIdFromMetricsDescriptor(descriptor);
            if (executionIdOfInterest.equals(executionId)) {
                compressor.addLong(addPrefixFn.apply(descriptor), value);
            }
        }

        @Override
        public void collectDouble(MetricDescriptor descriptor, double value) {
            Long executionId = JobMetricsUtil.getExecutionIdFromMetricsDescriptor(descriptor);
            if (executionIdOfInterest.equals(executionId)) {
                compressor.addDouble(addPrefixFn.apply(descriptor), value);
            }
        }

        @Override
        public void collectException(MetricDescriptor descriptor, Exception e) {
            Long executionId = JobMetricsUtil.getExecutionIdFromMetricsDescriptor(descriptor);
            if (executionIdOfInterest.equals(executionId)) {
                logger.warning("Exception when rendering job metrics: " + e, e);
            }
        }

        @Override
        public void collectNoValue(MetricDescriptor descriptor) {

        }

        @Nonnull
        public RawJobMetrics getMetrics() {
            return RawJobMetrics.of(compressor.getBlobAndReset());
        }

    }
}
