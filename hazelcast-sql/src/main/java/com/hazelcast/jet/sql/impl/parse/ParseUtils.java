/*
 * Copyright 2023 Hazelcast Inc.
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

import com.hazelcast.jet.config.ProcessingGuarantee;
import org.apache.calcite.sql.validate.SqlValidator;

import static com.hazelcast.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.config.ProcessingGuarantee.NONE;
import static com.hazelcast.jet.sql.impl.parse.ParserResource.RESOURCE;

public class ParseUtils {

    private ParseUtils() {
    }

    static ProcessingGuarantee parseProcessingGuarantee(SqlValidator validator, SqlOption option) {
        switch (option.valueString()) {
            case "exactlyOnce":
                return EXACTLY_ONCE;
            case "atLeastOnce":
                return AT_LEAST_ONCE;
            case "none":
                return NONE;
            default:
                throw validator.newValidationError(option.value(),
                        RESOURCE.processingGuaranteeBadValue(option.keyString(), option.valueString()));
        }
    }

    static long parseLong(SqlValidator validator, SqlOption option) {
        try {
            return Long.parseLong(option.valueString());
        } catch (NumberFormatException e) {
            throw validator.newValidationError(option.value(),
                    RESOURCE.jobOptionIncorrectNumber(option.keyString(), option.valueString()));
        }
    }
}
