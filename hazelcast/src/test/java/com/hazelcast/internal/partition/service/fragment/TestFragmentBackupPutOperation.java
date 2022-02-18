/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.service.fragment;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.services.ServiceNamespaceAware;

import java.io.IOException;

public class TestFragmentBackupPutOperation extends TestAbstractFragmentOperation implements ServiceNamespaceAware {

    private int value;

    public TestFragmentBackupPutOperation() {
    }

    public TestFragmentBackupPutOperation(String name, int value) {
        super(name);
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        TestFragmentedMigrationAwareService service = getService();
        service.put(name, getPartitionId(), value);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = in.readInt();
    }
}
