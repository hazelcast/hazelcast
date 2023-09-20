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

import com.hazelcast.cluster.Address;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.security.Permission;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A {@link ProcessorMetaSupplier} which wraps another {@code
 * ProcessorMetaSupplier} with one that will wrap its processors using
 * {@code wrapperSupplier}.
 */
public final class WrappingProcessorMetaSupplier implements ProcessorMetaSupplier, IdentifiedDataSerializable {

    private ProcessorMetaSupplier wrapped;
    private FunctionEx<Processor, Processor> wrapperSupplier;

    public WrappingProcessorMetaSupplier() { }

    public WrappingProcessorMetaSupplier(
            ProcessorMetaSupplier wrapped,
            FunctionEx<Processor, Processor> wrapperSupplier
    ) {
        this.wrapped = wrapped;
        this.wrapperSupplier = wrapperSupplier;
    }

    @Override
    public int preferredLocalParallelism() {
        return wrapped.preferredLocalParallelism();
    }

    @Nonnull @Override
    public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        Function<? super Address, ? extends ProcessorSupplier> function = wrapped.get(addresses);
        return address -> new WrappingProcessorSupplier(function.apply(address), wrapperSupplier);
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        wrapped.init(context);
    }

    @Nonnull
    @Override
    public Map<String, String> getTags() {
        return wrapped.getTags();
    }

    @Override
    public void close(@Nullable Throwable error) throws Exception {
        wrapped.close(error);
    }

    @Override
    public Permission getRequiredPermission() {
        return wrapped.getRequiredPermission();
    }

    @Override
    public boolean isReusable() {
        return wrapped.isReusable();
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
        return JetInitDataSerializerHook.WRAPPING_PROCESSOR_META_SUPPLIER;
    }
}
