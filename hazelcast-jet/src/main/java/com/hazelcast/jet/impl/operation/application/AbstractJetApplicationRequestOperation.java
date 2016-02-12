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

package com.hazelcast.jet.impl.operation.application;

import com.hazelcast.jet.api.JetApplicationManager;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.hazelcast.JetService;
import com.hazelcast.jet.spi.JetException;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;

public abstract class AbstractJetApplicationRequestOperation extends AbstractOperation {
    protected String name;
    protected Object result = true;
    protected JetApplicationConfig config;

    protected AbstractJetApplicationRequestOperation() {
    }

    protected AbstractJetApplicationRequestOperation(String name) {
        this(name, null);
    }

    protected AbstractJetApplicationRequestOperation(String name, JetApplicationConfig config) {
        this.name = name;
        this.config = config;
    }

    @Override
    public void beforeRun() throws Exception {

    }

    @Override
    public void run() throws Exception {

    }

    @Override
    public void afterRun() throws Exception {

    }

    public boolean returnsResponse() {
        return true;
    }

    public String getName() {
        return this.name;
    }

    protected ApplicationContext resolveApplicationContext() throws JetException {
        JetService jetService = getService();
        Address applicationOwner = getCallerAddress();

        if (applicationOwner == null) {
            applicationOwner = getNodeEngine().getThisAddress();
        }

        JetApplicationManager jetApplicationManager = jetService.getApplicationManager();
        ApplicationContext applicationContext = jetApplicationManager.createOrGetApplicationContext(this.name, this.config);

        if (!applicationContext.validateOwner(applicationOwner)) {
            throw new JetException(
                    "Invalid applicationOwner for applicationId "
                            + this.name
                            + " applicationOwner=" + applicationOwner
                            + " current=" + applicationContext.getOwner()
            );
        }

        return applicationContext;
    }

    public Object getResponse() {
        return this.result;
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
