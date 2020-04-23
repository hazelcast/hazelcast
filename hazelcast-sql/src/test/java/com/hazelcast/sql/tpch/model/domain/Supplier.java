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
import java.math.BigDecimal;

/**
 * TPC-H model: supplier.
 */
@SuppressWarnings("checkstyle:ParameterName")
public class Supplier implements DataSerializable {
    public long s_suppkey;
    public String s_name;
    public String s_address;
    public long s_nationkey;
    public String s_phone;
    public BigDecimal s_acctbal;
    public String s_comment;

    public Supplier() {
        // No-op.
    }

    public Supplier(
        long s_suppkey,
        String s_name,
        String s_address,
        long s_nationkey,
        String s_phone,
        BigDecimal s_acctbal,
        String s_comment
    ) {
        this.s_suppkey = s_suppkey;
        this.s_name = s_name;
        this.s_address = s_address;
        this.s_nationkey = s_nationkey;
        this.s_phone = s_phone;
        this.s_acctbal = s_acctbal;
        this.s_comment = s_comment;
    }

    public long getS_suppkey() {
        return s_suppkey;
    }

    public String getS_name() {
        return s_name;
    }

    public String getS_address() {
        return s_address;
    }

    public long getS_nationkey() {
        return s_nationkey;
    }

    public String getS_phone() {
        return s_phone;
    }

    public BigDecimal getS_acctbal() {
        return s_acctbal;
    }

    public String getS_comment() {
        return s_comment;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(s_suppkey);
        out.writeUTF(s_name);
        out.writeUTF(s_address);
        out.writeLong(s_nationkey);
        out.writeUTF(s_phone);
        out.writeObject(s_acctbal);
        out.writeUTF(s_comment);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        s_suppkey = in.readLong();
        s_name = in.readUTF();
        s_address = in.readUTF();
        s_nationkey = in.readLong();
        s_phone = in.readUTF();
        s_acctbal = in.readObject();
        s_comment = in.readUTF();
    }
}
