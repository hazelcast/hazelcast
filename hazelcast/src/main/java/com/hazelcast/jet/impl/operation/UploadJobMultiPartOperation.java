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
import com.hazelcast.jet.impl.jobupload.JobMultiPartParameterObject;
import com.hazelcast.jet.impl.jobupload.JobMetaDataParameterObject;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Resumes the execution of a suspended job.
 */
public class UploadJobMultiPartOperation extends AsyncJobOperation {

    JobMultiPartParameterObject jobMultiPartParameterObject;

    public UploadJobMultiPartOperation() {
    }

    public UploadJobMultiPartOperation(UUID sessionId, int currentPartNumber, int totalPartNumber, byte[] partData,
                                       int partSize) {
        jobMultiPartParameterObject = new JobMultiPartParameterObject(sessionId, currentPartNumber, totalPartNumber,
                partData, partSize);
    }


    @Override
    public CompletableFuture<Boolean> doRun() {
        return CompletableFuture.supplyAsync(() -> {
            JetServiceBackend jetServiceBackend = getJetServiceBackend();
            JobMetaDataParameterObject partsComplete = jetServiceBackend.storeJobMultiPart(jobMultiPartParameterObject);
            if (partsComplete != null) {
                jetServiceBackend.executeJar(partsComplete);
            }
            return true;
        });
    }


    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.UPLOAD_JOB_MULTIPART_OP;
    }
}
