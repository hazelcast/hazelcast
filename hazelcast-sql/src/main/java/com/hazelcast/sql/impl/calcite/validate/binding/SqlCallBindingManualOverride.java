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

package com.hazelcast.sql.impl.calcite.validate.binding;

import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlOperatorTable;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperator;

/**
 * A marker interface that could be applied to Hazelcast operators defined in the {@link HazelcastSqlOperatorTable}.
 * <p>
 * By using this interface a developer confirms that he will not use {@link SqlCallBindingOverrideOperandChecker}, but
 * instead will replace the original binding passed to {@link SqlOperator#checkOperandTypes(SqlCallBinding, boolean)}
 * with {@link SqlCallBindingOverride} manually.
 */
public interface SqlCallBindingManualOverride {
    // No-op
}
