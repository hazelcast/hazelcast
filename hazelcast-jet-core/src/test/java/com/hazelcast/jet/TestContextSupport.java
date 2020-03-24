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

package com.hazelcast.jet;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestProcessorSupplierContext;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

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
            return delegate.get(count);
        }

        @Override
        public void init(@Nonnull Context context) throws Exception {
            if (context instanceof TestProcessorSupplierContext) {
                TestProcessorSupplierContext c = (TestProcessorSupplierContext) context;
                NodeEngine nodeEngine = ((HazelcastInstanceImpl) c.jetInstance().getHazelcastInstance()).node.nodeEngine;
                context = new ProcCtx(c.jetInstance(), c.jobId(), c.executionId(), c.jobConfig(),
                        c.logger(), c.vertexName(), 1, 1, c.processingGuarantee(),
                        c.localParallelism(), 1, c.memberCount(), new ConcurrentHashMap<>(),
                        (InternalSerializationService) nodeEngine.getSerializationService());
            }
            delegate.init(context);
        }
    }
}
