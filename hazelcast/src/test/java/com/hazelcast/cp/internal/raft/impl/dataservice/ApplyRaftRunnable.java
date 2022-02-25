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

package com.hazelcast.cp.internal.raft.impl.dataservice;

import com.hazelcast.cp.internal.raft.impl.testing.RaftRunnable;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class ApplyRaftRunnable implements RaftRunnable, DataSerializable {

    private Object val;

    public ApplyRaftRunnable() {
    }

    public ApplyRaftRunnable(Object val) {
        this.val = val;
    }

    @Override
    public Object run(Object service, long commitIndex) {
        return ((RaftDataService) service).apply(commitIndex, val);
    }

    public Object getVal() {
        return val;
    }

    @Override
    public String toString() {
        return "ApplyRaftRunnable{" + "val=" + val + '}';
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(val);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        val = in.readObject();
    }

}
