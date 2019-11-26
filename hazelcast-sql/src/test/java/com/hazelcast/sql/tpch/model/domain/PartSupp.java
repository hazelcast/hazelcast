/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
 * TPC-H model: partsupp.
 */
@SuppressWarnings("checkstyle:ParameterName")
public class PartSupp implements Serializable {
    public int ps_availqty;
    public BigDecimal ps_supplycost;
    public String ps_comment;

    public PartSupp() {
        // No-op.
    }

    public PartSupp(int ps_availqty, BigDecimal ps_supplycost, String ps_comment) {
        this.ps_availqty = ps_availqty;
        this.ps_supplycost = ps_supplycost;
        this.ps_comment = ps_comment;
    }

    public static class Key implements Serializable {
        public long ps_partkey;
        public long ps_suppkey;

        public Key() {
            // No-op.
        }

        public Key(long ps_partkey, long ps_suppkey) {
            this.ps_partkey = ps_partkey;
            this.ps_suppkey = ps_suppkey;
        }
    }
}
