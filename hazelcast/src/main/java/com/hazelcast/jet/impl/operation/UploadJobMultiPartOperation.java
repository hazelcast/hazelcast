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

import com.hazelcast.client.impl.protocol.codec.JetUploadJobMultipartCodec;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.submitjob.memberside.JobMultiPartParameterObject;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;
import static com.hazelcast.jet.impl.util.Util.checkJetIsEnabled;

/**
 * Resumes the execution of a suspended job.
 */
public class UploadJobMultiPartOperation extends Operation implements IdentifiedDataSerializable {

    JobMultiPartParameterObject jobMultiPartParameterObject;

    public UploadJobMultiPartOperation() {
    }

    public UploadJobMultiPartOperation(JetUploadJobMultipartCodec.RequestParameters parameters) {
        // Save the parameters received from client
        jobMultiPartParameterObject = new JobMultiPartParameterObject();
        jobMultiPartParameterObject.setSessionId(parameters.sessionId);
        jobMultiPartParameterObject.setCurrentPartNumber(parameters.currentPartNumber);
        jobMultiPartParameterObject.setTotalPartNumber(parameters.totalPartNumber);
        jobMultiPartParameterObject.setPartData(parameters.partData);
        jobMultiPartParameterObject.setPartSize(parameters.partSize);
        jobMultiPartParameterObject.setSha256Hex(parameters.sha256Hex);
    }

    @Override
    public void run() {
        // Delegate to JetServiceBackend
        JetServiceBackend jetServiceBackend = getJetServiceBackend();
        jetServiceBackend.storeJobMultiPart(jobMultiPartParameterObject);
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
        return JetInitDataSerializerHook.UPLOAD_JOB_MULTIPART_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        writeUUID(out, jobMultiPartParameterObject.getSessionId());
        out.writeInt(jobMultiPartParameterObject.getCurrentPartNumber());
        out.writeInt(jobMultiPartParameterObject.getTotalPartNumber());
        out.writeByteArray(jobMultiPartParameterObject.getPartData());
        out.writeInt(jobMultiPartParameterObject.getPartSize());
        out.writeString(jobMultiPartParameterObject.getSha256Hex());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        jobMultiPartParameterObject = new JobMultiPartParameterObject();
        jobMultiPartParameterObject.setSessionId(readUUID(in));
        jobMultiPartParameterObject.setCurrentPartNumber(in.readInt());
        jobMultiPartParameterObject.setTotalPartNumber(in.readInt());
        jobMultiPartParameterObject.setPartData(in.readByteArray());
        jobMultiPartParameterObject.setPartSize(in.readInt());
        jobMultiPartParameterObject.setSha256Hex(in.readString());
    }
}
