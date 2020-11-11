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

package com.hazelcast.sql.impl.calcite.validate.types;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

// TODO: Rename or put somehwere else
public class CoercionUtils {

    public static RelDataType[] coercePlus(SqlCallBinding binding) {
        SqlValidator validator = binding.getValidator();
        SqlValidatorScope scope = binding.getScope();

        boolean hasParams = false;
        boolean hasLiterals = false;
        boolean hasOther = false;

        for (SqlNode operand : binding.operands()) {
            if (operand.getKind() == SqlKind.LITERAL) {
                hasLiterals = true;
            } else if (operand.getKind() == SqlKind.DYNAMIC_PARAM) {
                hasParams = true;
            } else {
                hasOther = true;
            }
        }

        if (hasParams && !hasOther && !hasLiterals) {
            throw QueryException.error(SqlErrorCode.PARSING, "PLUS cannot have parameters only");
        }

        return null;
    }
}
