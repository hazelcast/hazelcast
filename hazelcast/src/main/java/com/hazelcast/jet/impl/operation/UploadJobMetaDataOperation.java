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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeList;
import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;
import static com.hazelcast.jet.impl.util.Util.checkJetIsEnabled;

/**
 * Uploads the metadata of a job to be executed by a jar
 */
public class UploadJobMetaDataOperation extends Operation implements IdentifiedDataSerializable {

    boolean response;
    JobMetaDataParameterObject jobMetaDataParameterObject;

    public UploadJobMetaDataOperation() {
    }

    public UploadJobMetaDataOperation(JetUploadJobMetaDataCodec.RequestParameters parameters) {
        // Save the parameters received from client
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

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        writeUUID(out, jobMetaDataParameterObject.getSessionId());
        out.writeString(jobMetaDataParameterObject.getSha256Hex());
        out.writeString(jobMetaDataParameterObject.getFileName());
        out.writeString(jobMetaDataParameterObject.getSnapshotName());
        out.writeString(jobMetaDataParameterObject.getJobName());
        out.writeString(jobMetaDataParameterObject.getMainClass());
        writeList(jobMetaDataParameterObject.getJobParameters(), out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        jobMetaDataParameterObject = new JobMetaDataParameterObject();
        jobMetaDataParameterObject.setSessionId(readUUID(in));
        jobMetaDataParameterObject.setSha256Hex(in.readString());
        jobMetaDataParameterObject.setFileName(in.readString());
        jobMetaDataParameterObject.setSnapshotName(in.readString());
        jobMetaDataParameterObject.setJobName(in.readString());
        jobMetaDataParameterObject.setMainClass(in.readString());
        jobMetaDataParameterObject.setJobParameters(readList(in));
    }
}
