/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.management.operation;

import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;

/**
 * Operation to update Management Center URL configured on the node.
 */
public class UpdateManagementCenterUrlOperation extends AbstractManagementOperation {

    private static final int REDO_COUNT = 10;
    private static final int SLEEP_MILLIS = 1000;
    private String newUrl;

    @SuppressWarnings("unused")
    public UpdateManagementCenterUrlOperation() {
    }

    public UpdateManagementCenterUrlOperation(String newUrl) {
        this.newUrl = newUrl;
    }

    @Override
    public void run() throws Exception {
        ManagementCenterService service = ((NodeEngineImpl) getNodeEngine()).getManagementCenterService();
        int count = 0;
        while (service == null && count < REDO_COUNT) {
            Thread.sleep(SLEEP_MILLIS);
            count++;
            service = ((NodeEngineImpl) getNodeEngine()).getManagementCenterService();
        }

        if (service != null) {
            service.updateManagementCenterUrl(newUrl);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(newUrl);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        newUrl = in.readUTF();
    }

    @Override
    public int getId() {
        return ManagementDataSerializerHook.UPDATE_MANAGEMENT_CENTER_URL;
    }
}
