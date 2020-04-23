/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.tpch.model.domain;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * TPC-H model: nation.
 */
@SuppressWarnings("checkstyle:ParameterName")
public class Nation implements DataSerializable {
    public long n_nationkey;
    public String n_name;
    public long n_regionkey;
    public String n_comment;

    public Nation() {
        // No-op.
    }

    public Nation(long n_nationkey, String n_name, long n_regionkey, String n_comment) {
        this.n_nationkey = n_nationkey;
        this.n_name = n_name;
        this.n_regionkey = n_regionkey;
        this.n_comment = n_comment;
    }

    public long getN_nationkey() {
        return n_nationkey;
    }

    public String getN_name() {
        return n_name;
    }

    public long getN_regionkey() {
        return n_regionkey;
    }

    public String getN_comment() {
        return n_comment;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(n_nationkey);
        out.writeUTF(n_name);
        out.writeLong(n_regionkey);
        out.writeUTF(n_comment);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        n_nationkey = in.readLong();
        n_name = in.readUTF();
        n_regionkey = in.readLong();
        n_comment = in.readUTF();
    }
}
