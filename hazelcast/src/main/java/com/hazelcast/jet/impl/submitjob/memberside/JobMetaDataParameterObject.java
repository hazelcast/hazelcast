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

package com.hazelcast.jet.impl.submitjob.memberside;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

/**
 * Used by the member side as the wrapper for all parameters to run a jar that is on the client or on the member as Jet job
 */
public class JobMetaDataParameterObject {

    private UUID sessionId;

    private boolean jarOnMember;

    private String sha256Hex;

    private String fileName;

    private String snapshotName;

    private String jobName;

    private String mainClass;

    private List<String> jobParameters;

    private Path jarPath;

    private String uploadDirectoryPath;

    public UUID getSessionId() {
        return sessionId;
    }

    public void setSessionId(UUID sessionId) {
        this.sessionId = sessionId;
    }

    public boolean isJarOnMember() {
        return jarOnMember;
    }

    public boolean isJarOnClient() {
        return !jarOnMember;
    }

    public void setJarOnMember(boolean jarOnMember) {
        this.jarOnMember = jarOnMember;
    }

    public String getSha256Hex() {
        return sha256Hex;
    }

    public void setSha256Hex(String sha256Hex) {
        this.sha256Hex = sha256Hex;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public void setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }

    public List<String> getJobParameters() {
        return jobParameters;
    }

    public void setJobParameters(List<String> jobParameters) {
        this.jobParameters = jobParameters;
    }

    public Path getJarPath() {
        return jarPath;
    }

    public void setJarPath(Path jarPath) {
        this.jarPath = jarPath;
    }

    public String getUploadDirectoryPath() {
        return uploadDirectoryPath;
    }

    public void setUploadDirectoryPath(String uploadDirectoryPath) {
        this.uploadDirectoryPath = uploadDirectoryPath;
    }

    // Not all parameters need to be exposed
    // Show only relevant parameters that should be in the exception
    public String exceptionString() {
        return "JobMetaDataParameterObject{" +
               "jarOnMember='" + jarOnMember + '\'' +
               ", fileName='" + fileName + '\'' +
               ", snapshotName='" + snapshotName + '\'' +
               ", jobName='" + jobName + '\'' +
               ", mainClass='" + mainClass + '\'' +
               ", jobParameters=" + jobParameters +
               '}';
    }
}
