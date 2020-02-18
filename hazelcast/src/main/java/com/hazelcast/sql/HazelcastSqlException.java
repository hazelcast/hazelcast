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

package com.hazelcast.sql;

import com.hazelcast.core.HazelcastException;

import java.util.UUID;

/**
 * Exception occurred during SQL query execution.
 */
public class HazelcastSqlException extends HazelcastException {

    private static final long serialVersionUID = -1153559924521087582L;

    /** Code of the exception. */
    private int code;

    /** Originator member ID. */
    private UUID originatingMemberId;

    public HazelcastSqlException() {
        // No-op.
    }

    private HazelcastSqlException(int code, String message, Throwable cause, UUID originatingMemberId) {
        super(message, cause);

        this.code = code;
        this.originatingMemberId = originatingMemberId;
    }

    /**
     * Constructs a generic error.
     *
     * @param message Error message.
     * @return Exception object.
     */
    public static HazelcastSqlException error(String message) {
        return error(message, null);
    }

    /**
     * Constructs a generic error with the cause.
     *
     * @param message Error message.
     * @return Exception object.
     */
    public static HazelcastSqlException error(String message, Throwable cause) {
        return error(SqlErrorCode.GENERIC, message, cause);
    }

    /**
     * Constructs an error with specific code.
     *
     * @param code Error code.
     * @param message Error message.
     * @return Exception object.
     */
    public static HazelcastSqlException error(int code, String message) {
        return error(code, message, null);
    }

    /**
     * Constructs and error with specific code and cause.
     *
     * @param code Code.
     * @param message Message.
     * @param cause Cause.
     * @return Exception object.
     */
    public static HazelcastSqlException error(int code, String message, Throwable cause) {
        return new HazelcastSqlException(code, message, cause, null);
    }

    /**
     * Constructs an error which occurred on a remote member.
     *
     * @param code Code.
     * @param message Message.
     * @param originatingMemberId Originating member ID.
     * @return Exception object.
     */
    public static HazelcastSqlException remoteError(int code, String message, UUID originatingMemberId) {
        return new HazelcastSqlException(code, message, null, originatingMemberId);
    }

    /**
     * @return Code of the exception.
     */
    public int getCode() {
        return code;
    }

    public UUID getOriginatingMemberId() {
        return originatingMemberId;
    }

    // TODO: Override getMessage to output the code and originating member ID
}
