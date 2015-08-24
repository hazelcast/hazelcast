/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.template;

import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.annotation.Request;
import com.hazelcast.client.impl.protocol.ResponseMessageConst;

@GenerateCodec(id = TemplateConstants.TRANSACTION_TEMPLATE_ID, name = "Transaction", ns = "Transaction")
public interface TransactionalCodecTemplate {

    /**
     *
     * @param transactionId The internal Hazelcast transaction id.
     * @param threadId The thread id for the transaction.
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.VOID)
    void commit(String transactionId, long threadId);

    /**
     *
     * @param timeout The maximum allowed duration for the transaction operations.
     * @param durability The durability of the transaction
     * @param transactionType Identifies the type of the transaction. Possible values are:
     *                        1 (Two phase):  The two phase commit is more than the classic two phase commit (if you want a regular
     *                        two phase commit, use local). Before it commits, it copies the commit-log to other members, so in
     *                        case of member failure, another member can complete the commit.
     *                        2 (Local): Unlike the name suggests, local is a two phase commit. So first all cohorts are asked
     *                        to prepare if everyone agrees then all cohorts are asked to commit. The problem happens when during
     *                        the commit phase one or more members crash, that the system could be left in an inconsistent state.
     * @param threadId The thread id for the transaction.
     * @return The transaction id for the created transaction.
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.STRING)
    Object create(long timeout, int durability, int transactionType, long threadId);

    /**
     *
     * @param transactionId The internal Hazelcast transaction id.
     * @param threadId The thread id for the transaction.
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.VOID)
    void rollback(String transactionId, long threadId);
}

