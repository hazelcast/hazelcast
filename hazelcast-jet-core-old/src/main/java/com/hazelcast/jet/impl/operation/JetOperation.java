/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.job.JobService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import java.io.IOException;

public abstract class JetOperation extends Operation {

    protected String name;
    protected Object result = true;

    protected JetOperation() {
    }

    protected JetOperation(String name) {
        this.name = name;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    public String getJobName() {
        return this.name;
    }

    @Override
    public Object getResponse() {
        return this.result;
    }

    protected JobContext getJobContext() {
        JobService service = getService();
        JobContext jobContext = service.getContext(getJobName());

        if (jobContext == null) {
            throw new JetException("No job context found for job name -> " + getJobName());
        }

        validateJobContext(jobContext);
        return jobContext;
    }

    protected void validateJobContext(JobContext jobContext) {
        Address jobOwner = getJobOwner();
        if (!jobContext.validateOwner(jobOwner)) {
            throw new JetException("Invalid job owner for job name ->" + name
                    + ", job owner -> " + jobOwner + ", current owner -> " + jobContext.getOwner()
            );
        }
    }

    protected Address getJobOwner() {
        Address address = getCallerAddress();

        if (address == null) {
            return getNodeEngine().getThisAddress();
        }
        return address;
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(name);
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readObject();
    }
}
