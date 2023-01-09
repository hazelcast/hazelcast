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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.List;
import java.util.UUID;

import static com.hazelcast.jet.impl.util.Util.checkJetIsEnabled;

/**
 * Uploads the metadata of a job to be executed by a jar
 */
public class UploadJobMetaDataOperation extends Operation implements IdentifiedDataSerializable {

    Boolean response;
    JobMetaDataParameterObject jobMetaDataParameterObject;

    public UploadJobMetaDataOperation() {
    }

    public UploadJobMetaDataOperation(UUID sessionId, String sha256Hex, long jarSize, String snapshotName, String jobName,
                                      String mainClass, List<String> jobParameters) {
        jobMetaDataParameterObject = new JobMetaDataParameterObject();
        jobMetaDataParameterObject.setSessionId(sessionId);
        jobMetaDataParameterObject.setSha256Hex(sha256Hex);
        jobMetaDataParameterObject.setJarSize(jarSize);
        jobMetaDataParameterObject.setSnapshotName(snapshotName);
        jobMetaDataParameterObject.setJobName(jobName);
        jobMetaDataParameterObject.setMainClass(mainClass);
        jobMetaDataParameterObject.setJobParameters(jobParameters);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public void run() {

        JetServiceBackend jetServiceBackend = getJetServiceBackend();
        jetServiceBackend.checkIfCanExecuteJar();
        jetServiceBackend.storeJobMetaData(jobMetaDataParameterObject);
        response = true;

    }

    protected JetServiceBackend getJetServiceBackend() {
        checkJetIsEnabled(getNodeEngine());
        assert getServiceName().equals(JetServiceBackend.SERVICE_NAME) : "Service is not Jet Service";
        return getService();
    }

    @Override
    public final int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.UPLOAD_JOB_METADATA_OP;
    }
}
