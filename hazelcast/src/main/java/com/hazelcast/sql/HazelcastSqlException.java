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

package com.hazelcast.sql;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.spi.annotation.PrivateApi;

import javax.annotation.Nonnull;
import java.util.UUID;

/**
 * An exception occurred during SQL query execution.
 */
public class HazelcastSqlException extends HazelcastException {

    private final UUID originatingMemberId;
    private final int code;

    @PrivateApi
    public HazelcastSqlException(@Nonnull UUID originatingMemberId, int code, String message, Throwable cause) {
        super(message, cause);

        this.originatingMemberId = originatingMemberId;
        this.code = code;
    }

    /**
     * Gets ID of the member that caused or initiated an error condition.
     *
     * @return ID of the member that caused or initiated an error condition
     */
    @Nonnull
    public UUID getOriginatingMemberId() {
        return originatingMemberId;
    }

    /**
     * Gets the internal error code associated with the exception.
     *
     * @return the internal error code associated with the exception
     */
    @PrivateApi
    public int getCode() {
        return code;
    }
}
