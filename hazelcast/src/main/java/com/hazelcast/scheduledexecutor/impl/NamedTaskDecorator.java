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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.scheduledexecutor.NamedTask;

import java.io.IOException;
import java.util.concurrent.Callable;

public class NamedTaskDecorator<V>
        implements Runnable, Callable<V>, NamedTask, IdentifiedDataSerializable {

    private String name;

    private Object delegate;

    NamedTaskDecorator() {
    }

    private NamedTaskDecorator(String name, Runnable runnable) {
        this.name = name;
        this.delegate = runnable;
    }

    private NamedTaskDecorator(String name, Callable<V> callable) {
        this.name = name;
        this.delegate = callable;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void run() {
        ((Runnable) delegate).run();
    }

    @Override
    public V call()
            throws Exception {
        return ((Callable<V>) delegate).call();
    }

    public void initializeContext(ManagedContext context) {
        context.initialize(delegate);
    }

    @Override
    public int getFactoryId() {
        return ScheduledExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ScheduledExecutorDataSerializerHook.NAMED_TASK_DECORATOR;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(name);
        out.writeObject(delegate);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        name = in.readUTF();
        delegate = in.readObject();
    }

    public static Runnable named(String name, Runnable runnable) {
        return new NamedTaskDecorator(name, runnable);
    }

    public static <V> Callable<V> named(String name, Callable<V> callable) {
        return new NamedTaskDecorator<V>(name, callable);
    }

}
