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
import com.hazelcast.jet.config.ApplicationConfig;
import com.hazelcast.jet.impl.JetApplicationManager;
import com.hazelcast.jet.impl.application.ApplicationContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;

public class InitApplicationRequestOperation extends JetOperation {
    private ApplicationConfig config;

    @SuppressWarnings("unused")
    public InitApplicationRequestOperation() {
    }

    public InitApplicationRequestOperation(String name,
                                           ApplicationConfig config) {
        super(name);
        this.config = config;
    }

    @Override
    public void run() throws Exception {
        initializeApplicationContext();
        initializePartitions(getNodeEngine());
        getLogger().fine("InitApplicationRequestOperation");
    }

    private void initializePartitions(NodeEngine nodeEngine) {
        nodeEngine.getPartitionService().getMemberPartitionsMap();
    }

    private void initializeApplicationContext() throws JetException {
        JetApplicationManager jetApplicationManager = getApplicationManager();
        ApplicationContext applicationContext =
                jetApplicationManager.getOrCreateApplicationContext(this.name, this.config);
        validateApplicationContext(applicationContext);
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(config);
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.config = in.readObject();
    }
}
