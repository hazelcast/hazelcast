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

package com.hazelcast.jet.impl;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.version.Version;

import javax.security.auth.Subject;
import java.io.IOException;
import java.util.Set;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;

/**
 * Static metadata information about the job. There's one instance for each
 * jobId, used across multiple executions. The information is created initially
 * and never modified (unless we allow DAG updates in the future).
 */
public class JobRecord implements IdentifiedDataSerializable {

    private Version clusterVersion;
    private long jobId;
    private long creationTime;
    private Data dag;
    // JSON representation of DAG, used by Management Center
    private String dagJson;
    private JobConfig config;
    private Set<String> ownedObservables;
    private Subject subject;

    public JobRecord() {
    }

    public JobRecord(Version clusterVersion, long jobId, Data dag, String dagJson, JobConfig config,
                     Set<String> ownedObservables, Subject subject) {
        this.clusterVersion = clusterVersion;
        this.jobId = jobId;
        this.creationTime = Clock.currentTimeMillis();
        this.dag = dag;
        this.dagJson = dagJson;
        this.config = config;
        this.ownedObservables = ownedObservables;
        this.subject = subject;
    }

    public Version getClusterVersion() {
        return clusterVersion;
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

    // used by Management Center
    public String getDagJson() {
        return dagJson;
    }

    public JobConfig getConfig() {
        return config;
    }

    public Set<String> getOwnedObservables() {
        return ownedObservables;
    }

    public Subject getSubject() {
        return subject;
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.JOB_RECORD;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(clusterVersion);
        out.writeLong(jobId);
        out.writeLong(creationTime);
        IOUtil.writeData(out, dag);
        out.writeUTF(dagJson);
        out.writeObject(config);
        out.writeObject(ownedObservables);
        ImdgUtil.writeSubject(out, subject);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        clusterVersion = in.readObject();
        jobId = in.readLong();
        creationTime = in.readLong();
        dag = IOUtil.readData(in);
        dagJson = in.readUTF();
        config = in.readObject();
        ownedObservables = in.readObject();
        subject = ImdgUtil.readSubject(in);
    }

    @Override
    public String toString() {
        return "JobRecord{" +
                "jobId=" + idToString(jobId) +
                ", name=" + getConfig().getName() +
                ", creationTime=" + toLocalDateTime(creationTime) +
                ", dagJson=" + dagJson +
                ", config=" + config +
                ", ownedObservables=" + ownedObservables +
                '}';
    }
}
