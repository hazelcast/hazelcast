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
import java.time.LocalDate;

/**
 * TPC-H model: lineitem.
 */
@SuppressWarnings("checkstyle:ParameterName")
public class LineItem implements DataSerializable {
    public long l_suppkey;
    public BigDecimal l_quantity;
    public BigDecimal l_extendedprice;
    public BigDecimal l_discount;
    public BigDecimal l_tax;
    public String l_returnflag;
    public String l_linestatus;
    public LocalDate l_shipdate;
    public LocalDate l_commitdate;
    public LocalDate l_receiptdate;
    public String l_shipinstruct;
    public String l_shipmode;
    public String l_comment;

    public LineItem() {
        // No-op.
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    public LineItem(
        long l_suppkey,
        BigDecimal l_quantity,
        BigDecimal l_extendedprice,
        BigDecimal l_discount,
        BigDecimal l_tax,
        String l_returnflag,
        String l_linestatus,
        LocalDate l_shipdate,
        LocalDate l_commitdate,
        LocalDate l_receiptdate,
        String l_shipinstruct,
        String l_shipmode,
        String l_comment
    ) {
        this.l_suppkey = l_suppkey;
        this.l_quantity = l_quantity;
        this.l_extendedprice = l_extendedprice;
        this.l_discount = l_discount;
        this.l_tax = l_tax;
        this.l_returnflag = l_returnflag;
        this.l_linestatus = l_linestatus;
        this.l_shipdate = l_shipdate;
        this.l_commitdate = l_commitdate;
        this.l_receiptdate = l_receiptdate;
        this.l_shipinstruct = l_shipinstruct;
        this.l_shipmode = l_shipmode;
        this.l_comment = l_comment;
    }

    public long getL_suppkey() {
        return l_suppkey;
    }

    public BigDecimal getL_quantity() {
        return l_quantity;
    }

    public BigDecimal getL_extendedprice() {
        return l_extendedprice;
    }

    public BigDecimal getL_discount() {
        return l_discount;
    }

    public BigDecimal getL_tax() {
        return l_tax;
    }

    public String getL_returnflag() {
        return l_returnflag;
    }

    public String getL_linestatus() {
        return l_linestatus;
    }

    public LocalDate getL_shipdate() {
        return l_shipdate;
    }

    public LocalDate getL_commitdate() {
        return l_commitdate;
    }

    public LocalDate getL_receiptdate() {
        return l_receiptdate;
    }

    public String getL_shipinstruct() {
        return l_shipinstruct;
    }

    public String getL_shipmode() {
        return l_shipmode;
    }

    public String getL_comment() {
        return l_comment;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(l_suppkey);
        out.writeObject(l_quantity);
        out.writeObject(l_extendedprice);
        out.writeObject(l_discount);
        out.writeObject(l_tax);
        out.writeUTF(l_returnflag);
        out.writeUTF(l_linestatus);
        out.writeObject(l_shipdate);
        out.writeObject(l_commitdate);
        out.writeObject(l_receiptdate);
        out.writeUTF(l_shipinstruct);
        out.writeUTF(l_shipmode);
        out.writeUTF(l_comment);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        l_suppkey = in.readLong();
        l_quantity = in.readObject();
        l_extendedprice = in.readObject();
        l_discount = in.readObject();
        l_tax = in.readObject();
        l_returnflag = in.readUTF();
        l_linestatus = in.readUTF();
        l_shipdate = in.readObject();
        l_commitdate = in.readObject();
        l_receiptdate = in.readObject();
        l_shipinstruct = in.readUTF();
        l_shipmode = in.readUTF();
        l_comment = in.readUTF();
    }

    public static final class Key implements DataSerializable {
        public long l_orderkey;
        public long l_partkey;
        public long l_linenumber;

        public Key() {
            // No-op.
        }

        public Key(long l_orderkey, long l_partkey, long l_linenumber) {
            this.l_orderkey = l_orderkey;
            this.l_partkey = l_partkey;
            this.l_linenumber = l_linenumber;
        }

        public long getL_orderkey() {
            return l_orderkey;
        }

        public long getL_partkey() {
            return l_partkey;
        }

        public long getL_linenumber() {
            return l_linenumber;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(l_orderkey);
            out.writeLong(l_partkey);
            out.writeLong(l_linenumber);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            l_orderkey = in.readLong();
            l_partkey = in.readLong();
            l_linenumber = in.readLong();
        }
    }
}
