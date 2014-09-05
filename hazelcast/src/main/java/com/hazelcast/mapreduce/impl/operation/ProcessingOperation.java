/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.impl.operation;

import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;

/**
 * This is the base class for all map reduce framework operations, it always contains the name of
 * the JobTracker and the unique jobId
 */
public abstract class ProcessingOperation
        extends AbstractOperation
        implements IdentifiedDataSerializable {

    private transient String name;
    private transient String jobId;

    public ProcessingOperation() {
    }

    public ProcessingOperation(String name, String jobId) {
        this.name = name;
        this.jobId = jobId;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public String getServiceName() {
        return MapReduceService.SERVICE_NAME;
    }

    public String getName() {
        return name;
    }

    public String getJobId() {
        return jobId;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeUTF(jobId);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        jobId = in.readUTF();
    }

}
