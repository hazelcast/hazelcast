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

package com.hazelcast.sql.impl.calcite;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;

public class HazelcastConvertletTable implements SqlRexConvertletTable {

    private final SqlRexConvertletTable delegate;

    public HazelcastConvertletTable(SqlRexConvertletTable delegate) {
        this.delegate = delegate;
    }

    @Override
    public SqlRexConvertlet get(SqlCall call) {
        if (call.getOperator() instanceof SqlRexConvertlet) {
            return (SqlRexConvertlet) call.getOperator();
        }

        return delegate.get(call);
    }
}
