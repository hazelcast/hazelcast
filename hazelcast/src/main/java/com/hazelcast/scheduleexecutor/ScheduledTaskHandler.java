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

import com.hazelcast.spi.annotation.Beta;

@Beta
public class ScheduledTaskHandler {

    private final static String URN_BASE = "urn:hzScheduledTaskHandler:";

    private final static char DESC_SEP = '\0';

    private final int partitionId;

    private final String schedulerName;

    private final String taskName;

    private ScheduledTaskHandler(int partitionId, String schedulerName, String taskName) {
        this.partitionId = partitionId;
        this.schedulerName = schedulerName;
        this.taskName = taskName;
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
                + String.valueOf(partitionId) + DESC_SEP
                + schedulerName + DESC_SEP
                + taskName;
    }

    @Override
    public String toString() {
        return new StringBuilder(ScheduledTaskHandler.class.getName())
                .append("{ ")
                .append(schedulerName + ":" + taskName + "@" + partitionId)
                .append(" }")
                .toString();
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
        if (parts.length != 3) {
            throw new IllegalArgumentException("Wrong URN format.");
        }

        int partitionId = Integer.parseInt(parts[0]);
        String scheduler = parts[1];
        String task = parts[2];

        return new ScheduledTaskHandler(partitionId, scheduler, task);
    }

}
