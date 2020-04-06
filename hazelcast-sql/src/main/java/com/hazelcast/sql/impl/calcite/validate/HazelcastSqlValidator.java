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

package com.hazelcast.sql.impl.calcite.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

public class HazelcastSqlValidator extends SqlValidatorImpl {
    private static final HazelcastResource HZ_RESOURCE = Resources.create(HazelcastResource.class);

    public HazelcastSqlValidator(
        SqlOperatorTable opTab,
        SqlValidatorCatalogReader catalogReader,
        RelDataTypeFactory typeFactory,
        SqlConformance conformance
    ) {
        super(opTab, catalogReader, typeFactory, conformance);
        setTypeCoercion(new HazelcastTypeCoercion(this));
    }

    @Override
    protected void validateSelect(SqlSelect select, RelDataType targetRowType) {
        super.validateSelect(select, targetRowType);

        SqlNode from = select.getFrom();

        if (from != null && from.getKind() == SqlKind.UNION)  {
            throw newValidationError(from, HZ_RESOURCE.unionNotSupported());
        }
    }
}
