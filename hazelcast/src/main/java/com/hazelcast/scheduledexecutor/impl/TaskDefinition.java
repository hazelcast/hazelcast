/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.namespace.NamespaceUtil;
import com.hazelcast.internal.namespace.impl.NodeEngineThreadLocalContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.NodeEngine;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class TaskDefinition<V>
        implements IdentifiedDataSerializable, Versioned {

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
        // RU_COMPAT_5_3
        // Write this field regardless of the version
        // Up to 5.3 (included) this field was never written because of missing Versioned
        // It was never read because of missing Versioned
        // In 5.4 we added Versioned, also 5.4 and above will read the field
        // In 5.3 and below when it receives a Versioned TaskDefinition it also expects the field
        out.writeBoolean(autoDisposable);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        type = Type.valueOf(in.readString());
        name = in.readString();
        NodeEngine engine = NodeEngineThreadLocalContext.getNodeEngineThreadLocalContext();
        String namespace = DistributedScheduledExecutorService.lookupNamespace(engine, name);
        command = NamespaceUtil.callWithNamespace(engine, namespace, in::readObject);
        initialDelay = in.readLong();
        period = in.readLong();
        unit = TimeUnit.valueOf(in.readString());

        // During RU from older version to 5.5
        // - the cluster version is set to older version,
        // - the 5.5 member writes the field & version
        // - if older member is 5.4 it writes the field & version
        // - if older member is up to 5.3 the field is not written and version is missing (due to missing Versioned)
        // - so if the version is present we need to read the field - it either comes from 5.4 older member,
        // or 5.5 member
        if (!in.getVersion().isUnknown()) {
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
