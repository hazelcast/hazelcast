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

package com.hazelcast.jet;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

/**
 * The parameter object for {{@link JetService#submitJobFromJar(SubmitJobParameters)}}
 */
public class SubmitJobParameters {

    /**
     * Path of the jar
     */
    private Path jarPath;

    /**
     * Snapshot name to be used for the job
     */
    private String snapshotName;

    /**
     * Name to be used for the job
     */
    private String jobName;

    /**
     * Canonical name of the main class to be used for the job. For example "org.example.Main"
     */
    private String mainClass;

    /**
     * Parameters to be used for the job
     */
    private List<String> jobParameters = Collections.emptyList();

    public Path getJarPath() {
        return jarPath;
    }

    public SubmitJobParameters setJarPath(@Nonnull Path jarPath) {
        this.jarPath = jarPath;
        return this;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public SubmitJobParameters setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
        return this;
    }

    public String getJobName() {
        return jobName;
    }

    public SubmitJobParameters setJobName(String jobName) {
        this.jobName = jobName;
        return this;
    }

    public String getMainClass() {
        return mainClass;
    }

    public SubmitJobParameters setMainClass(String mainClass) {
        this.mainClass = mainClass;
        return this;
    }

    public List<String> getJobParameters() {
        return jobParameters;
    }

    public SubmitJobParameters setJobParameters(List<String> jobParameters) {
        this.jobParameters = jobParameters;
        return this;
    }
}
