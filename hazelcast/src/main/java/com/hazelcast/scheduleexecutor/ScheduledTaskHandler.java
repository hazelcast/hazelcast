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

package com.hazelcast.scheduleexecutor;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.annotation.Beta;

import java.net.UnknownHostException;

@Beta
public class ScheduledTaskHandler {

    private final static String URN_BASE = "urn:hzScheduledTaskHandler:";

    private final static char DESC_SEP = '\0';

    private final Address address;

    private final int partitionId;

    private final String schedulerName;

    private final String taskName;

    private ScheduledTaskHandler(int partitionId, String schedulerName, String taskName) {
        this.partitionId = partitionId;
        this.schedulerName = schedulerName;
        this.taskName = taskName;
        this.address = null;
    }

    private ScheduledTaskHandler(Address addr, String schedulerName, String taskName) {
        this.partitionId = -1;
        this.schedulerName = schedulerName;
        this.taskName = taskName;
        this.address = addr;
    }

    public Address getAddress() {
        return address;
    }

    public final int getPartitionId() {
        return partitionId;
    }

    public final String getSchedulerName() {
        return schedulerName;
    }

    public final String getTaskName() {
        return taskName;
    }

    public final String toURN() {
        //TODO tkountis - Better naming than URN ?
        return URN_BASE
                + (address == null
                        ? "-"
                        : (address.getHost() + ":" + String.valueOf(address.getPort()))) + DESC_SEP
                + String.valueOf(partitionId) + DESC_SEP
                + schedulerName + DESC_SEP
                + taskName;
    }

    @Override
    public String toString() {
        return new StringBuilder(ScheduledTaskHandler.class.getName())
                .append("{ ")
                .append(schedulerName + ":" + taskName + "@" + (address != null ? address : partitionId))
                .append(" }")
                .toString();
    }

    public static ScheduledTaskHandler of(Address addr, String schedulerName, String taskName) {
        return new ScheduledTaskHandler(addr, schedulerName, taskName);
    }

    public static ScheduledTaskHandler of(int partitionId, String schedulerName, String taskName) {
        return new ScheduledTaskHandler(partitionId, schedulerName, taskName);
    }

    public static ScheduledTaskHandler of(String URN) {
        if (!URN.startsWith(URN_BASE)) {
            throw new IllegalArgumentException("Wrong URN format.");
        }

        // Get rid of URN base
        URN = URN.replace(URN_BASE, "");

        String[] parts = URN.split(String.valueOf(DESC_SEP));
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

        int partitionId = Integer.parseInt(parts[0]);
        String scheduler = parts[1];
        String task = parts[2];

        return addr != null
                ? new ScheduledTaskHandler(addr, scheduler, task)
                : new ScheduledTaskHandler(partitionId, scheduler, task);
    }

}
