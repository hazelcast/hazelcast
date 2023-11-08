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

package com.hazelcast.jet.core;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Permission;
import java.util.Collection;
import java.util.List;

public class TaskMaxProcessorSupplier implements ProcessorSupplier {
    private final int localParallelismForMember;
    private final ProcessorSupplier supplier;

    private int processorOrder;

    public TaskMaxProcessorSupplier(int localParallelismForMember, ProcessorSupplier supplier, int processorOrder) {
        this.localParallelismForMember = localParallelismForMember;
        this.supplier = supplier;
        this.processorOrder = processorOrder;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        supplier.init(context);
    }

    @Override
    public boolean initIsCooperative() {
        return supplier.initIsCooperative();
    }

    @Override
    public boolean closeIsCooperative() {
        return supplier.closeIsCooperative();
    }

    @Override
    public void close(@Nullable Throwable error) throws Exception {
        supplier.close(error);
    }

    @Nullable
    @Override
    public List<Permission> permissions() {
        return supplier.permissions();
    }

    @Override
    public boolean checkLocalParallelism() {
        return false;
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int ignored) {
        Collection<? extends Processor> processors = supplier.get(localParallelismForMember);
        for (Processor processor : processors) {
            setProcessorOrderByReflection(processor);
        }
        return processors;
    }

    @SuppressWarnings({"java:S108", "java:S3011"})
    private void setProcessorOrderByReflection(Processor processor) {
        try {
            Method setter = processor.getClass().getDeclaredMethod("setProcessorOrder", int.class);
            setter.setAccessible(true);
            setter.invoke(processor, processorOrder++);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ignore) {
        }
    }
}
