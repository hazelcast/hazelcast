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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Common processor supplier for transform-using-service processors
 */
public final class ProcessorSupplierWithService<S> implements ProcessorSupplier {

    static final long serialVersionUID = 1L;

    private final ServiceFactory<S> serviceFactory;
    private final BiFunction<? super ServiceFactory<S>, ? super S, ? extends Processor> createProcessorFn;
    private transient S service;

    private ProcessorSupplierWithService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull BiFunction<? super ServiceFactory<S>, ? super S, ? extends Processor> createProcessorFn
    ) {
        this.serviceFactory = serviceFactory;
        this.createProcessorFn = createProcessorFn;
    }

    @Override
    public void init(@Nonnull ProcessorSupplier.Context context) {
        if (serviceFactory.hasLocalSharing()) {
            service = serviceFactory.createFn().apply(new ServiceContextImpl(serviceFactory, context));
        }
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        return Stream.generate(() -> createProcessorFn.apply(serviceFactory, service))
                .limit(count)
                .collect(toList());
    }

    @Override
    public void close(Throwable error) {
        if (service != null) {
            serviceFactory.destroyFn().accept(service);
        }
    }

    @Nonnull
    public static <S> ProcessorSupplier supplierWithService(
            @Nonnull ServiceFactory<S> serviceFactory,
            @Nonnull BiFunctionEx<? super ServiceFactory<S>, ? super S, ? extends Processor> createProcessorFn
    ) {
        return new ProcessorSupplierWithService<>(serviceFactory, createProcessorFn);
    }
}
