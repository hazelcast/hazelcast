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

package com.hazelcast.sql.impl.state;

import com.hazelcast.sql.impl.QueryId;

import java.util.Collection;
import java.util.UUID;

/**
 * Callback invoked when the query execution is completed. The implementation must clear all resources associated with
 * the query.
 * <p>
 * The interface is used to avoid a cyclic dependency between {@link QueryState} and {@link QueryStateRegistry}.
 */
public interface QueryStateCompletionCallback {
    /**
     * Handle query completion.
     *
     * @param queryId Query ID.
     */
    void onCompleted(QueryId queryId);

    /**
     * Handle query error.
     *
     * @param queryId Query ID.
     * @param errorCode Error code.
     * @param errorMessage Error message.
     * @param originatingMemberId Originating member ID.
     * @param memberIds Members which should be notified.
     */
    void onError(QueryId queryId, int errorCode, String errorMessage, UUID originatingMemberId, Collection<UUID> memberIds);
}
