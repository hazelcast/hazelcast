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

package com.hazelcast.sql.impl.calcite.parse;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.impl.ParseException;

/**
 * An exception class which acts as a wrapper for Calcite SqlParseException.
 */
class WrappedSqlParseException extends SqlParseException  {

    WrappedSqlParseException(SqlParseException cause) {
        super(
                cause.getMessage(),
                cause.getPos(),
                cause.getExpectedTokenSequences(),
                cause.getTokenImages(),
                cause.getCause()
        );
    }

    /**
     * If the cause is an instance of org.apache.calcite.sql.parser.impl.ParseException,
     * it returns the first line of the original message which is trimmed from the long list
     * of expected tokens. Otherwise, it returns the original error message.
     *
     * @return The error message.
     */
    @Override
    public String getMessage() {
        if (getCause() instanceof ParseException) {
            return trimMessage(super.getMessage());
        }
        return super.getMessage();
    }

    private static String trimMessage(String message) {
        String eol = System.getProperty("line.separator", "\n");
        String[] parts = message.split(eol, 2);
        return parts[0];
    }
}
