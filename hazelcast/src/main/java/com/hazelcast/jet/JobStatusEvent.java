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

package com.hazelcast.jet;

import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

import static com.hazelcast.jet.Util.idToString;

/**
 * Holds information about a job's state (status) transition,
 * reason for the transition, and whether it is user-initiated.
 *
 * @see JobStatusListener
 * @since 5.3
 */
public class JobStatusEvent implements IdentifiedDataSerializable {
    private long jobId;
    private JobStatus previousStatus;
    private JobStatus newStatus;
    private String description;
    private boolean userRequested;

    public JobStatusEvent() { }

    public JobStatusEvent(long jobId, @Nonnull JobStatus previousStatus, @Nonnull JobStatus newStatus,
                          @Nullable String description, boolean userRequested) {
        this.jobId = jobId;
        this.previousStatus = previousStatus;
        this.newStatus = newStatus;
        this.description = description;
        this.userRequested = userRequested;
    }

    public long getJobId() {
        return jobId;
    }

    @Nonnull
    public JobStatus getPreviousStatus() {
        return previousStatus;
    }

    @Nonnull
    public JobStatus getNewStatus() {
        return newStatus;
    }

    /**
     * If the event is generated by the user, indicates the action;
     * if there is a failure, indicates the cause; otherwise, null.
     */
    @Nullable
    public String getDescription() {
        return description;
    }

    /**
     * Indicates whether the event is generated by the user via
     * {@link Job#suspend()}, {@link Job#resume()}, {@link Job#restart()},
     * {@link Job#cancel()}, {@link Job#exportSnapshot(String)}, or
     * {@link Job#cancelAndExportSnapshot(String)}.
     */
    public boolean isUserRequested() {
        return userRequested;
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.JOB_STATUS_EVENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(jobId);
        out.writeObject(previousStatus);
        out.writeObject(newStatus);
        out.writeString(description);
        out.writeBoolean(userRequested);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        previousStatus = in.readObject();
        newStatus = in.readObject();
        description = in.readString();
        userRequested = in.readBoolean();
    }

    @Override
    public String toString() {
        return "JobStatusEvent{jobId=" + idToString(jobId)
                + ", previousStatus=" + previousStatus
                + ", newStatus=" + newStatus
                + ", description=" + description
                + ", userRequested=" + userRequested + "}";
    }
}
