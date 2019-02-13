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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;

public final class Contexts {

    private Contexts() {
    }

    static class MetaSupplierCtx implements ProcessorMetaSupplier.Context {
        private final JetInstance jetInstance;
        private final long jobId;
        private final long executionId;
        private final JobConfig jobConfig;
        private final ILogger logger;
        private final String vertexName;
        private final int localParallelism;
        private final int totalParallelism;
        private final int memberCount;
        private final ProcessingGuarantee processingGuarantee;

        MetaSupplierCtx(JetInstance jetInstance, long jobId, long executionId, JobConfig jobConfig, ILogger logger,
                        String vertexName, int localParallelism, int totalParallelism, int memberCount,
                        ProcessingGuarantee processingGuarantee) {
            this.jetInstance = jetInstance;
            this.jobId = jobId;
            this.executionId = executionId;
            this.jobConfig = jobConfig;
            this.logger = logger;
            this.vertexName = vertexName;
            this.totalParallelism = totalParallelism;
            this.localParallelism = localParallelism;
            this.memberCount = memberCount;
            this.processingGuarantee = processingGuarantee;
        }

        @Nonnull
        @Override
        public JetInstance jetInstance() {
            return jetInstance;
        }

        @Override
        public long jobId() {
            return jobId;
        }

        @Override
        public long executionId() {
            return executionId;
        }

        @Override @Nonnull
        public JobConfig jobConfig() {
            return jobConfig;
        }

        @Override
        public int totalParallelism() {
            return totalParallelism;
        }

        @Override
        public int localParallelism() {
            return localParallelism;
        }

        @Override
        public int memberCount() {
            return memberCount;
        }

        @Nonnull @Override
        public String vertexName() {
            return vertexName;
        }

        @Nonnull @Override
        public ILogger logger() {
            return logger;
        }

        @Override
        public ProcessingGuarantee processingGuarantee() {
            return processingGuarantee;
        }
    }

    static class ProcSupplierCtx extends MetaSupplierCtx implements ProcessorSupplier.Context {

        private final int memberIndex;

        @SuppressWarnings("checkstyle:ParameterNumber")
        ProcSupplierCtx(
                JetInstance jetInstance, long jobId, long executionId, JobConfig jobConfig, ILogger logger,
                String vertexName, int localParallelism, int totalParallelism, int memberIndex, int memberCount,
                ProcessingGuarantee processingGuarantee) {
            super(jetInstance, jobId, executionId, jobConfig, logger, vertexName, localParallelism, totalParallelism,
                    memberCount, processingGuarantee);
            this.memberIndex = memberIndex;
        }

        @Override
        public int memberIndex() {
            return memberIndex;
        }
    }

    public static class ProcCtx extends ProcSupplierCtx implements Processor.Context {

        private final int localProcessorIndex;
        private final int globalProcessorIndex;

        @SuppressWarnings("checkstyle:ParameterNumber")
        public ProcCtx(JetInstance instance, long jobId, long executionId, JobConfig jobConfig,
                       ILogger logger, String vertexName, int localProcessorIndex,
                       int globalProcessorIndex, ProcessingGuarantee processingGuarantee, int localParallelism,
                       int memberIndex, int memberCount) {
            super(instance, jobId, executionId, jobConfig, logger, vertexName, localParallelism,
                    memberCount * localParallelism, memberIndex, memberCount, processingGuarantee);
            this.localProcessorIndex = localProcessorIndex;
            this.globalProcessorIndex = globalProcessorIndex;
        }

        @Override
        public int localProcessorIndex() {
            return localProcessorIndex;
        }

        @Override
        public int globalProcessorIndex() {
            return globalProcessorIndex;
        }
    }
}
