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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.client.impl.protocol.codec.JetUploadJobMetaDataCodec;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.jobupload.JobMetaDataParameterObject;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import static com.hazelcast.jet.impl.util.Util.checkJetIsEnabled;

/**
 * Uploads the metadata of a job to be executed by a jar
 */
public class UploadJobMetaDataOperation extends Operation implements IdentifiedDataSerializable {

    Boolean response = false;
    JobMetaDataParameterObject jobMetaDataParameterObject;

    public UploadJobMetaDataOperation() {
    }

    public UploadJobMetaDataOperation(JetUploadJobMetaDataCodec.RequestParameters parameters) {
        jobMetaDataParameterObject = new JobMetaDataParameterObject();
        jobMetaDataParameterObject.setSessionId(parameters.sessionId);
        jobMetaDataParameterObject.setSha256Hex(parameters.sha256Hex);
        jobMetaDataParameterObject.setFileName(parameters.fileName);
        jobMetaDataParameterObject.setSnapshotName(parameters.snapshotName);
        jobMetaDataParameterObject.setJobName(parameters.jobName);
        jobMetaDataParameterObject.setMainClass(parameters.mainClass);
        jobMetaDataParameterObject.setJobParameters(parameters.jobParameters);
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
