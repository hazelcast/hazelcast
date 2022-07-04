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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;

import java.io.IOException;
import java.util.UUID;

public final class ScheduledTaskHandlerImpl
        extends ScheduledTaskHandler {

    private static final String URN_BASE = "urn:hzScheduledTaskHandler:";
    private static final char DESC_SEP = '\0';
    private static final int URN_PARTS = 4;

    private UUID uuid;

    private int partitionId;

    private String schedulerName;

    private String taskName;

    public ScheduledTaskHandlerImpl() {
    }

    public ScheduledTaskHandlerImpl(UUID uuid, int partitionId, String schedulerName, String taskName) {
        super();
        this.uuid = uuid;
        this.partitionId = partitionId;
        this.schedulerName = schedulerName;
        this.taskName = taskName;
    }

    @Override
    public UUID getUuid() {
        return uuid;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String getSchedulerName() {
        return schedulerName;
    }

    @Override
    public String getTaskName() {
        return taskName;
    }

    @Override
    public boolean isAssignedToPartition() {
        return uuid == null;
    }

    @Override
    public boolean isAssignedToMember() {
        return uuid != null;
    }

    @Override
    public String toUrn() {
        return URN_BASE + (uuid == null ? "-" : uuid) + DESC_SEP
                + partitionId + DESC_SEP + schedulerName + DESC_SEP + taskName;
    }

    @Override
    public int getFactoryId() {
        return ScheduledExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ScheduledExecutorDataSerializerHook.TASK_HANDLER;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeString(toUrn());
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        ScheduledTaskHandler handler = of(in.readString());
        this.uuid = handler.getUuid();
        this.partitionId = handler.getPartitionId();
        this.schedulerName = handler.getSchedulerName();
        this.taskName = handler.getTaskName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ScheduledTaskHandlerImpl that = (ScheduledTaskHandlerImpl) o;

        if (partitionId != that.partitionId) {
            return false;
        }
        if (uuid != null ? !uuid.equals(that.uuid) : that.uuid != null) {
            return false;
        }
        if (!schedulerName.equals(that.schedulerName)) {
            return false;
        }
        return taskName.equals(that.taskName);
    }

    @Override
    public int hashCode() {
        int result = uuid != null ? uuid.hashCode() : 0;
        result = 31 * result + partitionId;
        result = 31 * result + schedulerName.hashCode();
        result = 31 * result + taskName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ScheduledTaskHandler{" + "uuid=" + uuid + ", partitionId=" + partitionId + ", schedulerName='"
                + schedulerName + '\'' + ", taskName='" + taskName + '\'' + '}';
    }

    void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public static ScheduledTaskHandler of(UUID uuid, String schedulerName, String taskName) {
        return new ScheduledTaskHandlerImpl(uuid, -1, schedulerName, taskName);
    }

    public static ScheduledTaskHandler of(int partitionId, String schedulerName, String taskName) {
        return new ScheduledTaskHandlerImpl(null, partitionId, schedulerName, taskName);
    }

    public static ScheduledTaskHandler of(String urn) {
        if (!urn.startsWith(ScheduledTaskHandlerImpl.URN_BASE)) {
            throw new IllegalArgumentException("Wrong urn format.");
        }

        // Get rid of urn base
        urn = urn.replace(ScheduledTaskHandlerImpl.URN_BASE, "");

        String[] parts = urn.split(String.valueOf(ScheduledTaskHandlerImpl.DESC_SEP));
        if (parts.length != URN_PARTS) {
            throw new IllegalArgumentException("Wrong urn format.");
        }

        UUID uuid = null;
        if (!"-".equals(parts[0])) {
            uuid = UUID.fromString(parts[0]);
        }

        int partitionId = Integer.parseInt(parts[1]);
        String scheduler = parts[2];
        String task = parts[3];

        return new ScheduledTaskHandlerImpl(uuid, partitionId, scheduler, task);
    }
}
