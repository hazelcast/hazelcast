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

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;

import static com.hazelcast.jet.impl.util.Util.idToString;

public class JobRecord implements IdentifiedDataSerializable {

    private long jobId;
    private long creationTime;
    private Data dag;
    private JobConfig config;
    private int quorumSize;

    public JobRecord() {
    }

    public JobRecord(long jobId, long creationTime, Data dag, JobConfig config, int quorumSize) {
        this.jobId = jobId;
        this.creationTime = creationTime;
        this.dag = dag;
        this.config = config;
        this.quorumSize = quorumSize;
    }

    public long getJobId() {
        return jobId;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public Data getDag() {
        return dag;
    }

    public JobConfig getConfig() {
        return config;
    }

    public int getQuorumSize() {
        return quorumSize;
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
        out.writeObject(config);
        out.writeInt(quorumSize);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        jobId = in.readLong();
        creationTime = in.readLong();
        dag = in.readData();
        config = in.readObject();
        quorumSize = in.readInt();
    }

    @Override
    public String toString() {
        return "JobRecord{" +
                "jobId=" + idToString(jobId) +
                ", creationTime=" + Instant.ofEpochMilli(creationTime).atZone(ZoneId.systemDefault()) +
                ", dag=" + dag +
                ", config=" + config +
                ", quorumSize=" + quorumSize +
                '}';
    }

}
