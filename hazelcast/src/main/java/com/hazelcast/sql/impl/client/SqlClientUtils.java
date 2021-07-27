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

package com.hazelcast.sql.impl.client;

import com.hazelcast.logging.ILogger;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlExpectedResultType;
import com.hazelcast.sql.impl.QueryUtils;

import java.util.UUID;

/**
 * Static helpers for SQL client.
 */
public final class SqlClientUtils {

    private static final byte EXPECTED_RESULT_TYPE_ANY = 0;
    private static final byte EXPECTED_RESULT_TYPE_ROWS = 1;
    private static final byte EXPECTED_RESULT_TYPE_UPDATE_COUNT = 2;

    private SqlClientUtils() {
        // No-op.
    }

    public static SqlError exceptionToClientError(ILogger logger, Exception exception, UUID localMemberId) {
        logger.fine("query failed", exception);
        HazelcastSqlException sqlException = QueryUtils.toPublicException(exception, localMemberId);

        return new SqlError(
            sqlException.getCode(),
            sqlException.getMessage(),
            sqlException.getOriginatingMemberId()
        );
    }

    public static byte expectedResultTypeToByte(SqlExpectedResultType expectedResultType) {
        switch (expectedResultType) {
            case ANY:
                return EXPECTED_RESULT_TYPE_ANY;

            case ROWS:
                return EXPECTED_RESULT_TYPE_ROWS;

            default:
                assert expectedResultType == SqlExpectedResultType.UPDATE_COUNT;

                return EXPECTED_RESULT_TYPE_UPDATE_COUNT;
        }
    }

    public static SqlExpectedResultType expectedResultTypeToEnum(byte expectedResultType) {
        switch (expectedResultType) {
            case EXPECTED_RESULT_TYPE_ANY:
                return SqlExpectedResultType.ANY;

            case EXPECTED_RESULT_TYPE_ROWS:
                return SqlExpectedResultType.ROWS;

            default:
                assert expectedResultType == EXPECTED_RESULT_TYPE_UPDATE_COUNT;

                return SqlExpectedResultType.UPDATE_COUNT;
        }
    }
}
