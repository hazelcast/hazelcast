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

package com.hazelcast.sql.impl.calcite.parse;

import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * Encapsulates the result of a query conversion from SQL tree to a relational tree.
 */
public class QueryConvertResult {

    private final RelNode rel;
    private final List<String> fieldNames;

    public QueryConvertResult(RelNode rel, List<String> fieldNames) {
        this.rel = rel;
        this.fieldNames = fieldNames;
    }

    public RelNode getRel() {
        return rel;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }
}
