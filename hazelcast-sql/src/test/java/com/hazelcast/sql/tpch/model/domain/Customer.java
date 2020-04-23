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
 * TPC-H model: customer.
 */
@SuppressWarnings("checkstyle:ParameterName")
public class Customer implements DataSerializable {
    public String c_name;
    public String c_address;
    public long c_nationkey;
    public String c_phone;
    public BigDecimal c_acctbal;
    public String c_mktsegment;
    public String c_comment;

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(c_name);
        out.writeUTF(c_address);
        out.writeLong(c_nationkey);
        out.writeUTF(c_phone);
        out.writeObject(c_acctbal);
        out.writeUTF(c_mktsegment);
        out.writeUTF(c_comment);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        c_name = in.readUTF();
        c_address = in.readUTF();
        c_nationkey = in.readLong();
        c_phone = in.readUTF();
        c_acctbal = in.readObject();
        c_mktsegment = in.readUTF();
        c_comment = in.readUTF();
    }

    public Customer() {
        // No-op.
    }

    public Customer(
        String c_name,
        String c_address,
        long c_nationkey,
        String c_phone,
        BigDecimal c_acctbal,
        String c_mktsegment,
        String c_comment
    ) {
        this.c_name = c_name;
        this.c_address = c_address;
        this.c_nationkey = c_nationkey;
        this.c_phone = c_phone;
        this.c_acctbal = c_acctbal;
        this.c_mktsegment = c_mktsegment;
        this.c_comment = c_comment;
    }

    public String getC_name() {
        return c_name;
    }

    public String getC_address() {
        return c_address;
    }

    public long getC_nationkey() {
        return c_nationkey;
    }

    public String getC_phone() {
        return c_phone;
    }

    public BigDecimal getC_acctbal() {
        return c_acctbal;
    }

    public String getC_mktsegment() {
        return c_mktsegment;
    }

    public String getC_comment() {
        return c_comment;
    }
}
