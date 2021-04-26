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

package com.hazelcast.sql.impl.calcite.validate.types;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;

public class HazelcastIntegerTypeNameSpec extends SqlBasicTypeNameSpec {

    private final RelDataType type;

    public HazelcastIntegerTypeNameSpec(HazelcastIntegerType type) {
        super(type.getSqlTypeName(), type.getBitWidth(), SqlParserPos.ZERO);

        this.type = type;
    }

    @Override
    public RelDataType deriveType(SqlValidator validator) {
        return type;
    }
}
