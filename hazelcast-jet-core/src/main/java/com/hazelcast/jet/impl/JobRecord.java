/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;

public class JobRecord implements IdentifiedDataSerializable {

    private long jobId;
    private long creationTime;
    private Data dag;
    // JSON representation of DAG, used by management center
    private String dagJson;
    private JobConfig config;
    private int quorumSize;
    private boolean suspended;

    public JobRecord() {
    }

    public JobRecord(long jobId, long creationTime, Data dag, String dagJson, JobConfig config, int quorumSize,
                     boolean suspended) {
        this.jobId = jobId;
        this.creationTime = creationTime;
        this.dag = dag;
        this.dagJson = dagJson;
        this.config = config;
        this.quorumSize = quorumSize;
        this.suspended = suspended;
    }

    public long getJobId() {
        return jobId;
    }

    public String getJobNameOrId() {
        return config.getName() != null ? config.getName() : idToString(jobId);
    }

    public long getCreationTime() {
        return creationTime;
    }

    public Data getDag() {
        return dag;
    }

    public String getDagJson() {
        return dagJson;
    }

    public JobConfig getConfig() {
        return config;
    }

    public int getQuorumSize() {
        return quorumSize;
    }

    public boolean isSuspended() {
        return suspended;
    }

    public JobRecord withQuorumSize(int newQuorumSize) {
        if (newQuorumSize <= quorumSize) {
            throw new IllegalArgumentException("New quorum size: " + newQuorumSize
                    + " must be bigger than current quorum size of " + this);
        }
        return new JobRecord(getJobId(), getCreationTime(), getDag(), getDagJson(), getConfig(), newQuorumSize,
                isSuspended());
    }

    public JobRecord withSuspended(boolean suspended) {
        return new JobRecord(getJobId(), getCreationTime(), getDag(), getDagJson(), getConfig(), getQuorumSize(),
                suspended);
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.JOB_RECORD;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(jobId);
        out.writeLong(creationTime);
        out.writeData(dag);
        out.writeUTF(dagJson);
        out.writeObject(config);
        out.writeInt(quorumSize);
        out.writeBoolean(suspended);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        creationTime = in.readLong();
        dag = in.readData();
        dagJson = in.readUTF();
        config = in.readObject();
        quorumSize = in.readInt();
        suspended = in.readBoolean();
    }

    @Override
    public String toString() {
        return "JobRecord{" +
                "jobId=" + idToString(jobId) +
                ", name=" + getConfig().getName() +
                ", creationTime=" + toLocalDateTime(creationTime) +
                ", dagJson=" + dagJson +
                ", config=" + config +
                ", quorumSize=" + quorumSize +
                ", suspended=" + suspended +
                '}';
    }
}
