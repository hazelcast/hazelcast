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
import com.hazelcast.jet.impl.JetApplicationManager;
import com.hazelcast.jet.impl.application.ApplicationContext;
import com.hazelcast.jet.impl.hazelcast.JetService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;

public abstract class JetOperation extends AbstractOperation {

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

    public String getApplicationName() {
        return this.name;
    }

    @Override
    public Object getResponse() {
        return this.result;
    }

    protected ApplicationContext getApplicationContext() {
        JetApplicationManager jetApplicationManager = getApplicationManager();
        ApplicationContext applicationContext = jetApplicationManager.getApplicationContext(getApplicationName());

        if (applicationContext == null) {
            throw new JetException("No application context found for applicationId=" + getApplicationName());
        }

        validateApplicationContext(applicationContext);
        return applicationContext;
    }

    protected JetApplicationManager getApplicationManager() {
        JetService service = getService();
        return service.getApplicationManager();
    }

    protected void validateApplicationContext(ApplicationContext applicationContext) {
        Address applicationOwner = getApplicationOwner();
        if (!applicationContext.validateOwner(applicationOwner)) {
            throw new JetException(
                    "Invalid applicationOwner for applicationId "
                            + this.name
                            + " applicationOwner=" + applicationOwner
                            + " current=" + applicationContext.getOwner()
            );
        }
    }

    protected Address getApplicationOwner() {
        Address address = getCallerAddress();

        if (address == null) {
            return getNodeEngine().getThisAddress();
        }
        return address;
    }

    @Override
    public void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(this.name);
    }

    @Override
    public void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.name = in.readObject();
    }
}
