/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;

/**
 * A class that allows to override the {@link #deriveTypeImpl(SqlValidatorScope, SqlNode)} method, which is package-private
 * in the parent class.
 */
public class SqlValidatorImplBridge extends SqlValidatorImpl {
    public SqlValidatorImplBridge(
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            Config config
    ) {
        super(opTab, catalogReader, typeFactory, config);
    }

    @Override
    public RelDataType deriveTypeImpl(SqlValidatorScope scope, SqlNode operand) {
        return super.deriveTypeImpl(scope, operand);
    }
}
