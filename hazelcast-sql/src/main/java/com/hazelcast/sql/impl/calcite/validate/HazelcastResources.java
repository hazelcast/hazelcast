/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.validate;

import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.validate.SqlValidatorException;

public final class HazelcastResources {

    public static final HazelcastResource RESOURCE = Resources.create(HazelcastResource.class);

    private HazelcastResources() {
        // No-op.
    }

    public interface HazelcastResource {
        @Resources.BaseMessage("Function ''{0}'' does not exist")
        Resources.ExInst<CalciteException> functionDoesNotExist(String a0);

        @Resources.BaseMessage("No function matches {0} name and argument types (consider adding an explicit CAST)")
        Resources.ExInst<SqlValidatorException> canNotApplyOp2Type(String a0);
    }
}
