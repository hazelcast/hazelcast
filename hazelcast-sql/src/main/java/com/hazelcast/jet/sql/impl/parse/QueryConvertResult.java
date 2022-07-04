/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.parse;

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
