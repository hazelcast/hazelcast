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

package com.hazelcast.jet;

import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

@BinaryInterface
public class JobEvent implements IdentifiedDataSerializable {
    private long jobId;
    private JobStatus oldStatus;
    private JobStatus newStatus;
    private String description;
    private boolean userRequested;

    public JobEvent() { }

    public JobEvent(long jobId, @Nonnull JobStatus oldStatus, @Nonnull JobStatus newStatus,
                    @Nullable String description, boolean userRequested) {
        this.jobId = jobId;
        this.oldStatus = oldStatus;
        this.newStatus = newStatus;
        this.description = description;
        this.userRequested = userRequested;
    }

    public long getJobId() {
        return jobId;
    }

    @Nonnull
    public JobStatus getOldStatus() {
        return oldStatus;
    }

    @Nonnull
    public JobStatus getNewStatus() {
        return newStatus;
    }

    /**
     * If the event is generated by the user, indicates the action, namely one
     * of SUSPEND, RESUME, RESTART and CANCEL; if there is a failure, indicates
     * the cause; otherwise, null.
     */
    @Nullable
    public String getDescription() {
        return description;
    }

    /**
     * Indicates whether the event is generated by the user via
     * {@link Job#suspend()}, {@link Job#resume()}, {@link Job#restart()} or
     * {@link Job#cancel()}.
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
        return JetInitDataSerializerHook.JOB_EVENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(jobId);
        out.writeObject(oldStatus);
        out.writeObject(newStatus);
        out.writeString(description);
        out.writeBoolean(userRequested);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        oldStatus = in.readObject();
        newStatus = in.readObject();
        description = in.readString();
        userRequested = in.readBoolean();
    }
}
