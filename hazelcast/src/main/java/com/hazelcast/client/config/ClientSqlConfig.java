/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

import com.hazelcast.internal.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Contains the SQL configuration for the client.
 *
 * @since 5.2
 */
public class ClientSqlConfig {
    private ClientSqlResubmissionMode resubmissionMode = ClientSqlResubmissionMode.NEVER;

    /**
     * Sets the resubmission mode for failing SQL queries. See {@link ClientSqlResubmissionMode}.
     * @since 5.2
     */
    @Nonnull
    public ClientSqlConfig setSqlResubmissionMode(ClientSqlResubmissionMode resubmissionMode) {
        Preconditions.checkNotNull(resubmissionMode, "resubmissionMode");
        this.resubmissionMode = resubmissionMode;
        return this;
    }

    /**
     * Returns the resubmission mode for failing SQL queries. See {@link ClientSqlResubmissionMode}.
     * @since 5.2
     */
    public ClientSqlResubmissionMode getResubmissionMode() {
        return resubmissionMode;
    }

    @Override
    public String toString() {
        return "ClientSqlConfig{"
                + "resubmissionMode=" + resubmissionMode
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClientSqlConfig that = (ClientSqlConfig) o;
        return resubmissionMode == that.resubmissionMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resubmissionMode);
    }
}
