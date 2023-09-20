/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Objects;
import java.util.UUID;

/**
 * A server-side error that is propagated to the client.
 */
public class SqlError {

    private final int code;
    private final String message;
    private final UUID originatingMemberId;
    private final boolean suggestionExists;
    private final String suggestion;
    private final boolean isCauseStackTraceExists;
    private final String causeStackTrace;

    public SqlError(int code, String message, UUID originatingMemberId) {
        this(code, message, originatingMemberId, false, null, false, null);
    }

    public SqlError(
            int code,
            String message,
            UUID originatingMemberId,
            boolean suggestionExists,
            String suggestion,
            boolean isCauseStackTraceExists,
            String causeStackTrace
    ) {
        this.code = code;
        this.message = message;
        this.originatingMemberId = originatingMemberId;
        this.suggestionExists = suggestionExists;
        this.suggestion = suggestion;
        this.isCauseStackTraceExists = isCauseStackTraceExists;
        this.causeStackTrace = causeStackTrace;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public UUID getOriginatingMemberId() {
        return originatingMemberId;
    }

    public String getSuggestion() {
        return suggestion;
    }


    public boolean isCauseStackTraceExists() {
        return isCauseStackTraceExists;
    }

    public String getCauseStackTrace() {
        return causeStackTrace;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlError sqlError = (SqlError) o;
        return code == sqlError.code
               && suggestionExists == sqlError.suggestionExists
               && isCauseStackTraceExists == sqlError.isCauseStackTraceExists
               && Objects.equals(message, sqlError.message)
               && Objects.equals(originatingMemberId, sqlError.originatingMemberId)
               && Objects.equals(suggestion, sqlError.suggestion)
               && Objects.equals(causeStackTrace, sqlError.causeStackTrace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, message, originatingMemberId, suggestionExists, suggestion, isCauseStackTraceExists,
                causeStackTrace);
    }
}
