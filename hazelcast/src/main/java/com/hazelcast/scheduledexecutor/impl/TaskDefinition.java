/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Created by Thomas Kountis.
 */
public class TaskDefinition<V>
        implements IdentifiedDataSerializable {

    public enum Type {

        SINGLE_RUN,

        WITH_REPETITION

    }

    private Type type;

    private String name;

    private Callable<V> command;

    private long initialDelay;

    private long delay;

    private long period;

    private TimeUnit unit;

    public TaskDefinition() {
    }

    public TaskDefinition(Type type, String name, Callable<V> command, long initialDelay,
                          TimeUnit unit) {
        this.type = type;
        this.name = name;
        this.command = command;
        this.initialDelay = initialDelay;
        this.unit = unit;
    }

    public TaskDefinition(Type type, String name, Callable<V> command, long initialDelay,
                          long delay, TimeUnit unit) {
        this.type = type;
        this.name = name;
        this.command = command;
        this.initialDelay = initialDelay;
        this.delay = delay;
        this.unit = unit;
    }

    public TaskDefinition(Type type, String name, Callable<V> command, long initialDelay,
                          long delay, long period, TimeUnit unit) {
        this.type = type;
        this.name = name;
        this.command = command;
        this.initialDelay = initialDelay;
        this.delay = delay;
        this.period = period;
        this.unit = unit;
    }

    public Type getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public Callable<V> getCommand() {
        return command;
    }

    public long getInitialDelay() {
        return initialDelay;
    }

    public long getDelay() {
        return delay;
    }

    public long getPeriod() {
        return period;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    @Override
    public int getFactoryId() {
        return ScheduledExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ScheduledExecutorDataSerializerHook.RUNNABLE_DEFINITION;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(type.name());
        out.writeUTF(name);
        out.writeObject(command);
        out.writeLong(initialDelay);
        out.writeLong(delay);
        out.writeLong(period);
        out.writeUTF(unit.name());
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        type = Type.valueOf(in.readUTF());
        name = in.readUTF();
        command = in.readObject();
        initialDelay = in.readLong();
        delay = in.readLong();
        period = in.readLong();
        unit = TimeUnit.valueOf(in.readUTF());
    }

    @Override
    public String toString() {
        return "TaskDefinition{" + "type=" + type + ", name='" + name + '\'' + ", command=" + command + ", initialDelay="
                + initialDelay + ", delay=" + delay + ", period=" + period + ", unit=" + unit + '}';
    }
}
