/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Function;

/**
 * A {@link ProcessorMetaSupplier} which wraps another {@code
 * ProcessorMetaSupplier} with one that will wrap its processors using
 * {@code wrapperSupplier}.
 */
public final class WrappingProcessorMetaSupplier implements ProcessorMetaSupplier {
    private ProcessorMetaSupplier wrapped;
    private DistributedFunction<Processor, Processor> wrapperSupplier;

    public WrappingProcessorMetaSupplier(ProcessorMetaSupplier wrapped,
                                         DistributedFunction<Processor, Processor> wrapperSupplier) {
        this.wrapped = wrapped;
        this.wrapperSupplier = wrapperSupplier;
    }

    @Override
    public int preferredLocalParallelism() {
        return wrapped.preferredLocalParallelism();
    }

    @Nonnull @Override
    public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        Function<Address, ProcessorSupplier> function = wrapped.get(addresses);
        return address -> new WrappingProcessorSupplier(function.apply(address), wrapperSupplier);
    }

    @Override
    public void init(@Nonnull Context context) {
        wrapped.init(context);
    }

    @Override
    public void close(Throwable error) throws Exception {
        wrapped.close(error);
    }
}
