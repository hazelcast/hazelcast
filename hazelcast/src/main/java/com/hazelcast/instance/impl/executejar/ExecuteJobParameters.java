/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance.impl.executejar;

import com.hazelcast.jet.Job;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ExecuteJobParameters {

    private String jarPath;

    private String snapshotName;

    private String jobName;

    private final CopyOnWriteArrayList<Job> submittedJobs = new CopyOnWriteArrayList<>();

    public ExecuteJobParameters() {
    }

    public ExecuteJobParameters(String jarPath, String snapshotName, String jobName) {
        this.jarPath = jarPath;
        this.snapshotName = snapshotName;
        this.jobName = jobName;
    }

    public String getJarPath() {
        return jarPath;
    }

    public boolean hasJarPath() {
        return jarPath != null;
    }

    public boolean hasSnapshotName() {
        return snapshotName != null;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public boolean hasJobName() {
        return jobName != null;
    }

    public String getJobName() {
        return jobName;
    }

    public List<Job> getSubmittedJobs() {
        return submittedJobs;
    }

    public void addSubmittedJob(Job job) {
        submittedJobs.add(job);
    }
}
