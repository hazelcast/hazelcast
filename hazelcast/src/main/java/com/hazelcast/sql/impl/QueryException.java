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

package com.hazelcast.sql.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.sql.SqlErrorCode;

import java.util.Collection;
import java.util.UUID;

/**
 * Exception occurred during SQL query execution.
 */
public final class QueryException extends HazelcastException {

    private final int code;
    private final UUID originatingMemberId;

    private QueryException(int code, String message, Throwable cause, UUID originatingMemberId) {
        super(message, cause);

        this.code = code;
        this.originatingMemberId = originatingMemberId;
    }

    public static QueryException error(String message) {
        return error(message, null);
    }

    public static QueryException error(String message, Throwable cause) {
        return error(SqlErrorCode.GENERIC, message, cause, null);
    }

    public static QueryException error(int code, String message) {
        return new QueryException(code, message, null, null);
    }

    public static QueryException error(int code, String message, Throwable cause) {
        return new QueryException(code, message, cause, null);
    }

    public static QueryException error(int code, String message, UUID originatingMemberId) {
        return new QueryException(code, message, null, originatingMemberId);
    }

    public static QueryException error(int code, String message, Throwable cause, UUID originatingMemberId) {
        return new QueryException(code, message, cause, originatingMemberId);
    }

    public static QueryException memberConnection(UUID memberId) {
        return error(SqlErrorCode.MEMBER_CONNECTION, "Connection to member is broken: " + memberId);
    }

    public static QueryException memberLeave(UUID memberId) {
        return error(SqlErrorCode.MEMBER_LEAVE, "Participating member has left the topology: " + memberId);
    }

    public static QueryException memberLeave(Collection<UUID> memberIds) {
        return error(SqlErrorCode.MEMBER_LEAVE, "Participating members has left the topology: " + memberIds);
    }

    public static QueryException timeout(long timeout) {
        return error(SqlErrorCode.TIMEOUT, "Query has been cancelled due to timeout (" + timeout + " ms)");
    }

    public static QueryException cancelledByUser() {
        return error(SqlErrorCode.CANCELLED_BY_USER, "Query was cancelled by user");
    }

    public static QueryException dataException(String message, Throwable cause) {
        return error(SqlErrorCode.DATA_EXCEPTION, message, cause);
    }

    public static QueryException dataException(String message) {
        return dataException(message, null);
    }

    /**
     * @return Code of the exception.
     */
    public int getCode() {
        return code;
    }

    /**
     * Get originator of the exception.
     *
     * @return ID of the member where the exception occurred or {@code null} if the exception was raised on a local member
     *         and is not propagated yet.
     */
    public UUID getOriginatingMemberId() {
        return originatingMemberId;
    }

    @Override
    public String getMessage() {
        if (originatingMemberId != null) {
            return super.getMessage() + " (code=" + code + ", originatingMemberId=" + originatingMemberId + ')';
        } else {
            return super.getMessage() + " (code=" + code + ')';
        }
    }
}
