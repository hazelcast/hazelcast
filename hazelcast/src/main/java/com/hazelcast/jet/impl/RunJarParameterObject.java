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

import java.util.List;

/**
 * The wrapper for all parameters to run an uploaded jar as Jet job
 */
public class RunJarParameterObject {

    private final String snapshotName;
    private final String jobName;

    private final String mainClass;

    private final List<String> jobParameters;

    private final byte[] jarData;

    public RunJarParameterObject(String snapshotName, String jobName, String mainClass, List<String> jobParameters,
                                 byte[] jarData) {
        this.snapshotName = snapshotName;
        this.jobName = jobName;
        this.mainClass = mainClass;
        this.jobParameters = jobParameters;
        this.jarData = jarData;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public String getJobName() {
        return jobName;
    }

    public String getMainClass() {
        return mainClass;
    }

    public List<String> getJobParameters() {
        return jobParameters;
    }

    public byte[] getJarData() {
        return jarData;
    }
}
