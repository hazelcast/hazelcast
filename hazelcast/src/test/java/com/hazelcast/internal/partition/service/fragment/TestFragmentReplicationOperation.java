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
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestFragmentReplicationOperation extends Operation {

    private Map<TestServiceNamespace, Integer> values;

    public TestFragmentReplicationOperation() {
    }

    public TestFragmentReplicationOperation(Map<TestServiceNamespace, Integer> values) {
        this.values = values;
    }


    @Override
    public void run() throws Exception {
        TestFragmentedMigrationAwareService service = getService();
        for (Map.Entry<TestServiceNamespace, Integer> entry : values.entrySet()) {
            service.put(entry.getKey().name, getPartitionId(), entry.getValue());
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeInt(values.size());
        for (Map.Entry<TestServiceNamespace, Integer> entry : values.entrySet()) {
            out.writeObject(entry.getKey());
            out.writeInt(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        int len = in.readInt();
        values = new HashMap<TestServiceNamespace, Integer>(len);
        for (int i = 0; i < len; i++) {
            TestServiceNamespace ns = in.readObject();
            int value = in.readInt();
            values.put(ns, value);
        }
    }
}
