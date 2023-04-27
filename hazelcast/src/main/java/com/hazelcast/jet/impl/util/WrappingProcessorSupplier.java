/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;

import static com.hazelcast.jet.impl.util.Util.toList;

/**
 * A {@link ProcessorSupplier} which wraps another {@code ProcessorSupplier}
 * with one that will wrap its processors using {@code wrapperSupplier}.
 */
public final class WrappingProcessorSupplier implements ProcessorSupplier, IdentifiedDataSerializable {

    private ProcessorSupplier wrapped;
    private FunctionEx<Processor, Processor> wrapperSupplier;

    public WrappingProcessorSupplier() { }

    public WrappingProcessorSupplier(
            ProcessorSupplier wrapped,
            FunctionEx<Processor, Processor> wrapperSupplier
    ) {
        this.wrapped = wrapped;
        this.wrapperSupplier = wrapperSupplier;
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        Collection<? extends Processor> processors = wrapped.get(count);
        return toList(processors, wrapperSupplier);
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        wrapped.init(context);
    }

    @Override
    public boolean initIsCooperative() {
        return wrapped.initIsCooperative();
    }

    @Override
    public boolean closeIsCooperative() {
        return wrapped.closeIsCooperative();
    }

    @Override
    public void close(Throwable error) throws Exception {
        wrapped.close(error);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(wrapped);
        out.writeObject(wrapperSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        wrapped = in.readObject();
        wrapperSupplier = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.WRAPPING_PROCESSOR_SUPPLIER;
    }
}
