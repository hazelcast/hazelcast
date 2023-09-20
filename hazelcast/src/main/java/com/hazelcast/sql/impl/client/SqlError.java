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

    @SuppressWarnings({
            "checkstyle:CyclomaticComplexity",
            "checkstyle:NPathComplexity",
    })
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SqlError sqlError = (SqlError) o;

        if (code != sqlError.code) {
            return false;
        }

        if (!message.equals(sqlError.message)) {
            return false;
        }

        if (!originatingMemberId.equals(sqlError.originatingMemberId)) {
            return false;
        }

        if (suggestionExists && sqlError.suggestionExists) {
            if (!Objects.equals(suggestion, sqlError.suggestion)) {
                return false;
            }
        }
        if (isCauseStackTraceExists && sqlError.isCauseStackTraceExists) {
            if (!Objects.equals(causeStackTrace, sqlError.causeStackTrace)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = code;

        result = 31 * result + message.hashCode();
        result = 31 * result + originatingMemberId.hashCode();
        result = 31 * result + Objects.hashCode(suggestion);
        result = 31 * result + Objects.hashCode(causeStackTrace);

        return result;
    }
}
