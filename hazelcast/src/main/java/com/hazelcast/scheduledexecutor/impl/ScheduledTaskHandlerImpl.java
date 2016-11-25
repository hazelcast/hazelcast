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

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;

import java.io.IOException;
import java.net.UnknownHostException;

/**
 * Created by Thomas Kountis.
 */
public final class ScheduledTaskHandlerImpl
        extends ScheduledTaskHandler {

    private final static String URN_BASE = "urn:hzScheduledTaskHandler:";

    private final static char DESC_SEP = '\0';

    private Address address;

    private int partitionId;

    private String schedulerName;

    private String taskName;

    public ScheduledTaskHandlerImpl() {
    }

    private ScheduledTaskHandlerImpl(int partitionId, String schedulerName, String taskName) {
        super();
        this.partitionId = partitionId;
        this.schedulerName = schedulerName;
        this.taskName = taskName;
        this.address = null;
    }

    private ScheduledTaskHandlerImpl(Address address, String schedulerName, String taskName) {
        super();
        this.address = address;
        this.schedulerName = schedulerName;
        this.taskName = taskName;
        this.partitionId = -1;
    }

    @Override
    public Address getAddress() {
        return address;
    }

    @Override
    public final int getPartitionId() {
        return partitionId;
    }

    @Override
    public final String getSchedulerName() {
        return schedulerName;
    }

    @Override
    public final String getTaskName() {
        return taskName;
    }

    @Override
    public final boolean isAssignedToPartition() {
        return address == null;
    }

    @Override
    public final boolean isAssignedToMember() {
        return address != null;
    }

    @Override
    public final String toURN() {
        return URN_BASE
                + (address == null
                        ? "-"
                        : (address.getHost() + ":" + String.valueOf(address.getPort()))) + DESC_SEP
                + String.valueOf(partitionId) + DESC_SEP
                + schedulerName + DESC_SEP
                + taskName;
    }

    @Override
    public int getFactoryId() {
        return ScheduledExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ScheduledExecutorDataSerializerHook.TASK_HANDLER;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(toURN());
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        ScheduledTaskHandler handler = of(in.readUTF());
        this.address = handler.getAddress();
        this.partitionId = handler.getPartitionId();
        this.schedulerName = handler.getSchedulerName();
        this.taskName = handler.getTaskName();
    }

    @Override
    public String toString() {
        return "ScheduledTaskHandler{" + "address=" + address + ", partitionId=" + partitionId + ", schedulerName='"
                + schedulerName + '\'' + ", taskName='" + taskName + '\'' + '}';
    }

    public static ScheduledTaskHandler of(Address addr, String schedulerName, String taskName) {
        return new ScheduledTaskHandlerImpl(addr, schedulerName, taskName);
    }

    public static ScheduledTaskHandler of(int partitionId, String schedulerName, String taskName) {
        return new ScheduledTaskHandlerImpl(partitionId, schedulerName, taskName);
    }

    public static ScheduledTaskHandler of(String URN) {
        if (!URN.startsWith(ScheduledTaskHandlerImpl.URN_BASE)) {
            throw new IllegalArgumentException("Wrong URN format.");
        }

        // Get rid of URN base
        URN = URN.replace(ScheduledTaskHandlerImpl.URN_BASE, "");

        String[] parts = URN.split(String.valueOf(ScheduledTaskHandlerImpl.DESC_SEP));
        if (parts.length != 4) {
            throw new IllegalArgumentException("Wrong URN format.");
        }

        Address addr = null;
        if (!"-".equals(parts[0])) {
            String[] hostParts = parts[0].split(":");
            try {
                addr = new Address(hostParts[0], Integer.parseInt(hostParts[1]));
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Wrong URN format.", e);
            }
        }

        int partitionId = Integer.parseInt(parts[1]);
        String scheduler = parts[2];
        String task = parts[3];

        return addr != null
                ? new ScheduledTaskHandlerImpl(addr, scheduler, task)
                : new ScheduledTaskHandlerImpl(partitionId, scheduler, task);
    }
}
