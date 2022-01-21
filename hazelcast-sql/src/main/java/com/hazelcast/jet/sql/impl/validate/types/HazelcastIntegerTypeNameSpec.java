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

package com.hazelcast.jet.sql.impl.validate.types;

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
