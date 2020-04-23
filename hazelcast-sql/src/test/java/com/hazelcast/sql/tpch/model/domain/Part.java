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
 * TPC-H model: part.
 */
@SuppressWarnings("checkstyle:ParameterName")
public class Part implements DataSerializable {
    public String p_name;
    public String p_mfgr;
    public String p_brand;
    public String p_type;
    public int p_size;
    public String p_container;
    public BigDecimal p_retailprice;
    public String p_comment;

    public Part() {
        // No-op.
    }

    public Part(
        String p_name,
        String p_mfgr,
        String p_brand,
        String p_type,
        int p_size,
        String p_container,
        BigDecimal p_retailprice,
        String p_comment
    ) {
        this.p_name = p_name;
        this.p_mfgr = p_mfgr;
        this.p_brand = p_brand;
        this.p_type = p_type;
        this.p_size = p_size;
        this.p_container = p_container;
        this.p_retailprice = p_retailprice;
        this.p_comment = p_comment;
    }

    public String getP_name() {
        return p_name;
    }

    public String getP_mfgr() {
        return p_mfgr;
    }

    public String getP_brand() {
        return p_brand;
    }

    public String getP_type() {
        return p_type;
    }

    public int getP_size() {
        return p_size;
    }

    public String getP_container() {
        return p_container;
    }

    public BigDecimal getP_retailprice() {
        return p_retailprice;
    }

    public String getP_comment() {
        return p_comment;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(p_name);
        out.writeUTF(p_mfgr);
        out.writeUTF(p_brand);
        out.writeUTF(p_type);
        out.writeInt(p_size);
        out.writeUTF(p_container);
        out.writeObject(p_retailprice);
        out.writeUTF(p_comment);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        p_name = in.readUTF();
        p_mfgr = in.readUTF();
        p_brand = in.readUTF();
        p_type = in.readUTF();
        p_size = in.readInt();
        p_container = in.readUTF();
        p_retailprice = in.readObject();
        p_comment = in.readUTF();
    }
}
