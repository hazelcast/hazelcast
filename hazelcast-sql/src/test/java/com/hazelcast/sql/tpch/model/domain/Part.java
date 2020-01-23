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
 * TPC-H model: part.
 */
@SuppressWarnings("checkstyle:ParameterName")
public class Part implements Serializable {
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
}
