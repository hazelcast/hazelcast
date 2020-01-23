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

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * TPC-H model: customer.
 */
@SuppressWarnings("checkstyle:ParameterName")
public class Customer implements Serializable {
    public String c_name;
    public String c_address;
    public long c_nationkey;
    public String c_phone;
    public BigDecimal c_acctbal;
    public String c_mktsegment;
    public String c_comment;

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
}
