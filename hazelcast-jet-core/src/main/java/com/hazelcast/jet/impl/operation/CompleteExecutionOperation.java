/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.renderers.ProbeRenderer;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.JobExecutionService;
import com.hazelcast.jet.impl.JobMetricsUtil;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.metrics.RawJobMetrics;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.ExceptionUtil.isRestartableException;
import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;

public class CompleteExecutionOperation extends Operation implements IdentifiedDataSerializable {

    private long executionId;
    private Throwable error;
    private RawJobMetrics response;

    public CompleteExecutionOperation() {
    }

    public CompleteExecutionOperation(long executionId, Throwable error) {
        this.executionId = executionId;
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
        JobMetricsRenderer metricsRenderer = new JobMetricsRenderer(executionId);
        nodeEngine.getMetricsRegistry().render(metricsRenderer);
        //TODO: we should probably filter out some of the metrics for completed jobs, not all make sense at this point
        //  take for example MetricNames.LAST_FORWARDED_WM_LATENCY
        response = metricsRenderer.getJobMetrics();

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
    public int getId() {
        return JetInitDataSerializerHook.COMPLETE_EXECUTION_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(executionId);
        out.writeObject(error);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        executionId = in.readLong();
        error = in.readObject();
    }

    private static class JobMetricsRenderer implements ProbeRenderer {

        private final Long executionIdOfInterest;
        private final Map<String, Long> jobMetrics = new HashMap<>();

        JobMetricsRenderer(long executionId) {
            this.executionIdOfInterest = executionId;
        }

        @Override
        public void renderLong(String name, long value) {
            Long executionId = JobMetricsUtil.getExecutionIdFromMetricDescriptor(name);
            if (executionIdOfInterest.equals(executionId)) {
                jobMetrics.put(name, value);
            }
        }

        @Override
        public void renderDouble(String name, double value) {
            renderLong(name, JobMetricsUtil.toLongMetricValue(value));
        }

        @Override
        public void renderException(String name, Exception e) {
        }

        @Override
        public void renderNoValue(String name) {
        }

        RawJobMetrics getJobMetrics() {
            return RawJobMetrics.of(System.currentTimeMillis(), jobMetrics);
        }
    }
}
