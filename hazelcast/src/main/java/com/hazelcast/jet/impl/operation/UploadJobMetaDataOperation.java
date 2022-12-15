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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.jobupload.JobMetaDataParameterObject;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Uploads the metadata of a job to be executed by a jar
 */
public class UploadJobMetaDataOperation extends AsyncJobOperation {

    JobMetaDataParameterObject jobMetaDataParameterObject;

    public UploadJobMetaDataOperation() {
    }

    public UploadJobMetaDataOperation(UUID sessionId, long jarSize, String snapshotName, String jobName, String mainClass,
                                      List<String> jobParameters) {
        jobMetaDataParameterObject = new JobMetaDataParameterObject();
        jobMetaDataParameterObject.setSessionId(sessionId);
        jobMetaDataParameterObject.setJarSize(jarSize);
        jobMetaDataParameterObject.setSnapshotName(snapshotName);
        jobMetaDataParameterObject.setJobName(jobName);
        jobMetaDataParameterObject.setMainClass(mainClass);
        jobMetaDataParameterObject.setJobParameters(jobParameters);
    }


    @Override
    public CompletableFuture<Boolean> doRun() {
        return CompletableFuture.supplyAsync(() -> {
            JetServiceBackend jetServiceBackend = getJetServiceBackend();
            jetServiceBackend.checkIfCanRunJar();
            jetServiceBackend.storeJobMetaData(jobMetaDataParameterObject);
            return true;
        });
    }


    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.UPLOAD_JOB_METADATA_OP;
    }
}
