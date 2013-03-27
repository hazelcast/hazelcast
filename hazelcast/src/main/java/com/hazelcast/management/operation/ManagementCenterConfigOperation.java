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

package com.hazelcast.management.operation;

import com.hazelcast.management.ManagementCenterService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;

/**
 * User: sancar
 * Date: 3/27/13
 * Time: 10:37 AM
 */
public class ManagementCenterConfigOperation extends Operation {

    private int redoCount = 10;
    private String newUrl;

    public ManagementCenterConfigOperation() {

    }

    public ManagementCenterConfigOperation(String newUrl) {
        this.newUrl = newUrl;
    }

    public void beforeRun() throws Exception {
    }

    public void run() throws Exception {
        ManagementCenterService service = ((NodeEngineImpl) getNodeEngine()).getManagementCenterService();
        int count = 0;
        while (service == null && count < redoCount) {
            Thread.sleep(1000);
            count++;
            service = ((NodeEngineImpl) getNodeEngine()).getManagementCenterService();
        }
        if (service != null)
            service.changeWebServerUrl(newUrl);
    }

    public void afterRun() throws Exception {
    }

    public boolean returnsResponse() {
        return false;
    }

    public Object getResponse() {
        return null;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(newUrl);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        newUrl = in.readUTF();
    }
}
