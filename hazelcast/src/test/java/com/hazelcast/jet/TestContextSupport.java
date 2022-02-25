/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestProcessorSupplierContext;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.impl.processor.ProcessorWrapper;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

/**
 * A rather hacky way to allow `TestSupport` usage with IMDG data structures
 * (those use to downcast contexts to {@link ProcSupplierCtx} to get access
 * to job {@link SerializationService}). Hopefully will be removed when
 * better/proper way of contextual serialization for IMDG is in place.
 */
public final class TestContextSupport {

    private TestContextSupport() {
    }

    public static ProcessorMetaSupplier adaptSupplier(ProcessorMetaSupplier processorMetaSupplier) {
        return new TestProcessorMetaSupplierAdapter(processorMetaSupplier);
    }

    public static ProcessorSupplier adaptSupplier(ProcessorSupplier processorSupplier) {
        return new TestProcessorSupplierAdapter(processorSupplier);
    }

    private static final class TestProcessorMetaSupplierAdapter implements ProcessorMetaSupplier {

        private final ProcessorMetaSupplier delegate;

        private TestProcessorMetaSupplierAdapter(ProcessorMetaSupplier delegate) {
            this.delegate = delegate;
        }

        @Nonnull
        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> new TestProcessorSupplierAdapter(delegate.get(addresses).apply(address));
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            delegate.init(context);
        }
    }

    private static final class TestProcessorSupplierAdapter implements ProcessorSupplier {

        private final ProcessorSupplier delegate;

        private TestProcessorSupplierAdapter(ProcessorSupplier delegate) {
            this.delegate = delegate;
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return delegate.get(count).stream().map(TestProcessorAdapter::new).collect(toList());
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            if (context instanceof TestProcessorSupplierContext) {
                TestProcessorSupplierContext c = (TestProcessorSupplierContext) context;
                NodeEngineImpl nodeEngine = Util.getNodeEngine(c.hazelcastInstance());
                context = new ProcCtx(nodeEngine, c.jobId(), c.executionId(), c.jobConfig(),
                        c.logger(), c.vertexName(), 1, 1,
                        c.isLightJob(), c.partitionAssignment(), c.localParallelism(), 1, c.memberCount(),
                        new ConcurrentHashMap<>(), (InternalSerializationService) nodeEngine.getSerializationService(),
                        null, context.classLoader());
            }
            delegate.init(context);
        }
    }

    private static final class TestProcessorAdapter extends ProcessorWrapper {

        private TestProcessorAdapter(Processor wrapped) {
            super(wrapped);
        }

        @Override
        protected Context initContext(Context context) {
            context = super.initContext(context);
            if (context instanceof TestProcessorContext) {
                TestProcessorContext c = (TestProcessorContext) context;
                NodeEngineImpl nodeEngine = Util.getNodeEngine(c.hazelcastInstance());
                context = new ProcCtx(nodeEngine, c.jobId(), c.executionId(), c.jobConfig(),
                        c.logger(), c.vertexName(), c.localProcessorIndex(), c.globalProcessorIndex(),
                        c.isLightJob(), c.partitionAssignment(), c.localParallelism(), c.memberIndex(), c.memberCount(),
                        new ConcurrentHashMap<>(), (InternalSerializationService) nodeEngine.getSerializationService(),
                        null, context.classLoader());
            }
            return context;
        }
    }
}
