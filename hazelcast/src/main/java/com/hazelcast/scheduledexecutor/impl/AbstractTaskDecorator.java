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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.concurrent.Callable;

public abstract class AbstractTaskDecorator<V>
        implements Runnable, Callable<V>, IdentifiedDataSerializable {

    protected Object delegate;


    AbstractTaskDecorator() {
    }

    AbstractTaskDecorator(Runnable runnable) {
        this.delegate = runnable;
    }

    AbstractTaskDecorator(Callable<V> callable) {
        this.delegate = callable;
    }

    @Override
    public void run() {
        ((Runnable) delegate).run();
    }

    @Override
    @SuppressWarnings("unchecked")
    public V call()
            throws Exception {
        return ((Callable<V>) delegate).call();
    }

    @Override
    public int getFactoryId() {
        return ScheduledExecutorDataSerializerHook.F_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeObject(delegate);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        delegate = in.readObject();
    }

    public boolean isDecoratedWith(Class<?> clazz) {
        if (clazz.isAssignableFrom(this.getClass())) {
           return true;
        }
        if (delegate instanceof AbstractTaskDecorator) {
            return ((AbstractTaskDecorator<?>) delegate).isDecoratedWith(clazz);
        }
        return clazz.isAssignableFrom(delegate.getClass());
    }

    @SuppressWarnings("unchecked")
    public <T> T undecorateTo(Class<T> clazz) {
        if (clazz.isAssignableFrom(this.getClass())) {
            return (T) this;
        }
        if (delegate instanceof AbstractTaskDecorator) {
            return (T) ((AbstractTaskDecorator) delegate).undecorateTo(clazz);
        }
        if (clazz.isAssignableFrom(delegate.getClass())) {
            return (T) delegate;
        }
        return null;
    }

    void initializeContext(ManagedContext context) {
        if (delegate instanceof AbstractTaskDecorator) {
            ((AbstractTaskDecorator) delegate).initializeContext(context);
        } else {
            delegate = context.initialize(delegate);
        }
    }

}
