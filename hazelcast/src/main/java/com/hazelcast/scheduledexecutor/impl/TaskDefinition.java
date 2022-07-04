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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class TaskDefinition<V>
        implements IdentifiedDataSerializable {

    public enum Type {

        SINGLE_RUN(0), AT_FIXED_RATE(1);

        private final byte id;

        Type(int status) {
            this.id = (byte) status;
        }

        public byte getId() {
            return id;
        }

        public static Type getById(int id) {
            for (Type as : values()) {
                if (as.getId() == id) {
                    return as;
                }
            }
            throw new IllegalArgumentException("Unsupported ID value");
        }
    }

    private Type type;
    private String name;
    private Callable<V> command;
    private long initialDelay;
    private long period;
    private TimeUnit unit;
    private boolean autoDisposable;

    public TaskDefinition() {
    }

    public TaskDefinition(Type type, String name, Callable<V> command, long delay, TimeUnit unit, boolean autoDisposable) {
        this.type = type;
        this.name = name;
        this.command = command;
        this.initialDelay = delay;
        this.unit = unit;
        this.autoDisposable = autoDisposable;
    }

    public TaskDefinition(Type type, String name, Callable<V> command, long initialDelay, long period,
                          TimeUnit unit, boolean autoDisposable) {
        this.type = type;
        this.name = name;
        this.command = command;
        this.initialDelay = initialDelay;
        this.period = period;
        this.unit = unit;
        this.autoDisposable = autoDisposable;
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

    public long getPeriod() {
        return period;
    }

    public TimeUnit getUnit() {
        return unit;
    }

    public boolean isAutoDisposable() {
        return autoDisposable;
    }

    @Override
    public int getFactoryId() {
        return ScheduledExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ScheduledExecutorDataSerializerHook.RUNNABLE_DEFINITION;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeString(type.name());
        out.writeString(name);
        out.writeObject(command);
        out.writeLong(initialDelay);
        out.writeLong(period);
        out.writeString(unit.name());
        if (out.getVersion().isGreaterOrEqual(Versions.V4_1)) {
            out.writeBoolean(autoDisposable);
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        type = Type.valueOf(in.readString());
        name = in.readString();
        command = in.readObject();
        initialDelay = in.readLong();
        period = in.readLong();
        unit = TimeUnit.valueOf(in.readString());
        if (in.getVersion().isGreaterOrEqual(Versions.V4_1)) {
            autoDisposable = in.readBoolean();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskDefinition<?> that = (TaskDefinition<?>) o;
        return initialDelay == that.initialDelay && period == that.period && type == that.type && name.equals(that.name)
                && unit == that.unit && autoDisposable == that.autoDisposable;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[]{type, name, initialDelay, period, unit, autoDisposable});
    }

    @Override
    public String toString() {
        return "TaskDefinition{"
                + "type=" + type
                + ", name='" + name + '\''
                + ", command=" + command
                + ", initialDelay=" + initialDelay
                + ", period=" + period
                + ", unit=" + unit
                + ", autoDisposable=" + autoDisposable
                + '}';
    }
}
