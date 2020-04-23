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
 * TPC-H model: region.
 */
@SuppressWarnings("checkstyle:ParameterName")
public class Region implements DataSerializable {
    public long r_regionkey;
    public String r_name;
    public String r_comment;

    public Region() {
        // No-op.
    }

    public Region(long r_regionkey, String r_name, String r_comment) {
        this.r_regionkey = r_regionkey;
        this.r_name = r_name;
        this.r_comment = r_comment;
    }

    public long getR_regionkey() {
        return r_regionkey;
    }

    public String getR_name() {
        return r_name;
    }

    public String getR_comment() {
        return r_comment;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(r_regionkey);
        out.writeUTF(r_name);
        out.writeUTF(r_comment);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        r_regionkey = in.readLong();
        r_name = in.readUTF();
        r_comment = in.readUTF();
    }
}
