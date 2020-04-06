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

package com.hazelcast.sql.impl.operation;

import java.util.UUID;

/**
 * Query operation executor.
 */
public interface QueryOperationHandler {
    /**
     * Submit operation for execution on a member.
     *
     * @param sourceMemberId Source member ID.
     * @param targetMemberId Target member ID.
     * @param operation Operation to be executed.
     * @return {@code true} if operation is triggered, {@code false} if target member is not available.
     */
    boolean submit(UUID sourceMemberId, UUID targetMemberId, QueryOperation operation);

    /**
     * Execute the operation synchronously.
     *
     * @param operation Operation.
     */
    void execute(QueryOperation operation);

    /**
     * Create a channel for ordered operation scheduling.
     *
     * @param sourceMemberId Source member ID.
     * @param targetMemberId Target member ID.
     * @return Channel.
     */
    QueryOperationChannel createChannel(UUID sourceMemberId, UUID targetMemberId);
}
