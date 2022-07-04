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

package com.hazelcast.cp.internal.raft.impl.testing;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class TestRaftGroupId implements CPGroupId, DataSerializable {

    private String name;

    public TestRaftGroupId() {
    }

    public TestRaftGroupId(String name) {
        assert name != null;
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getId() {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TestRaftGroupId)) {
            return false;
        }

        TestRaftGroupId that = (TestRaftGroupId) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return "TestRaftGroupId{" + "name='" + name + "\''}";
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
    }
}
