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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.scheduledexecutor.NamedTask;

import java.io.IOException;
import java.util.concurrent.Callable;

//RU_COMPAT_5_3 "implements Versioned" can be removed in 5.5
public class NamedTaskDecorator<V> extends AbstractTaskDecorator<V>
        implements NamedTask, Versioned {

    private String name;

    NamedTaskDecorator() {
    }

    private NamedTaskDecorator(String name, Runnable runnable) {
        super(runnable);
        this.name = name;
    }

    private NamedTaskDecorator(String name, Callable<V> callable) {
        super(callable);
        this.name = name;
    }


    @Override
    public String getName() {
        return name;
    }


    @Override
    public int getClassId() {
        return ScheduledExecutorDataSerializerHook.NAMED_TASK_DECORATOR;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        out.writeString(name);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        name = in.readString();
    }

    public static Runnable named(String name, Runnable runnable) {
        return new NamedTaskDecorator(name, runnable);
    }

    public static <V> Callable<V> named(String name, Callable<V> callable) {
        return new NamedTaskDecorator<V>(name, callable);
    }
}
