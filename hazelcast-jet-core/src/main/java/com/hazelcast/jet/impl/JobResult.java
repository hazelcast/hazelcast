/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.Util.idToString;

public class JobResult implements IdentifiedDataSerializable {

    private String coordinatorUUID;
    private long jobId;
    private long creationTime;
    private long completionTime;
    private Throwable failure;

    public JobResult() {
    }

    public JobResult(long jobId, String coordinatorUUID, long creationTime, Long completionTime, Throwable failure) {
        this.jobId = jobId;
        this.coordinatorUUID = coordinatorUUID;
        this.creationTime = creationTime;
        this.completionTime = completionTime;
        this.failure = failure;
    }

    public long getJobId() {
        return jobId;
    }

    public String getCoordinatorUUID() {
        return coordinatorUUID;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getCompletionTime() {
        return completionTime;
    }

    public boolean isSuccessful() {
        return (failure == null);
    }
    public boolean isSuccessfulOrCancelled() {
        return (failure == null || failure instanceof CancellationException);
    }

    public Throwable getFailure() {
        return failure;
    }

    public CompletableFuture<Boolean> asCompletableFuture() {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        if (failure == null) {
            future.complete(true);
        } else {
            future.completeExceptionally(failure);
        }

        return future;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JobResult jobResult = (JobResult) o;

        if (jobId != jobResult.jobId) {
            return false;
        }
        if (creationTime != jobResult.creationTime) {
            return false;
        }
        if (completionTime != jobResult.completionTime) {
            return false;
        }
        if (!coordinatorUUID.equals(jobResult.coordinatorUUID)) {
            return false;
        }
        return failure.equals(jobResult.failure);
    }

    @Override public int hashCode() {
        int result = coordinatorUUID.hashCode();
        result = 31 * result + (int) (jobId ^ (jobId >>> 32));
        result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
        result = 31 * result + (int) (completionTime ^ (completionTime >>> 32));
        result = 31 * result + failure.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "JobResult{" +
                "coordinatorUUID='" + coordinatorUUID + '\'' +
                ", jobId=" + idToString(jobId) +
                ", creationTime=" + Instant.ofEpochMilli(creationTime).atZone(ZoneId.systemDefault()) +
                ", completionTime=" + Instant.ofEpochMilli(completionTime).atZone(ZoneId.systemDefault()) +
                ", failure=" + failure +
                '}';
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.JOB_RESULT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(jobId);
        out.writeUTF(coordinatorUUID);
        out.writeLong(creationTime);
        out.writeLong(completionTime);
        out.writeObject(failure);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        coordinatorUUID = in.readUTF();
        creationTime = in.readLong();
        completionTime = in.readLong();
        failure = in.readObject();
    }

}
