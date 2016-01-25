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

import javax.transaction.xa.Xid;

@GenerateCodec(id = TemplateConstants.XA_TRANSACTION_TEMPLATE_ID, name = "XATransaction",
        ns = "Hazelcast.Client.Protocol.Codec")
public interface XATransactionalCodecTemplate {

    /**
     *
     * @param xid Java XA transaction id as defined in interface javax.transaction.xa.Xid.
     */
    @Request(id = 1, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "xid")
    void clearRemote(Xid xid);

    /**
     *
     * @return Array of Xids.
     */
    @Request(id = 2, retryable = false, response = ResponseMessageConst.LIST_DATA)
    Object collectTransactions();

    /**
     *
     * @param xid Java XA transaction id as defined in interface javax.transaction.xa.Xid.
     * @param isCommit If true, the transaction is committed else transaction is rolled back.
     */
    @Request(id = 3, retryable = false, response = ResponseMessageConst.VOID, partitionIdentifier = "xid")
    void finalize(Xid xid, boolean isCommit);

    /**
     *
     * @param transactionId The internal Hazelcast transaction id.
     * @param onePhase If true, the prepare is also done.
     */
    @Request(id = 4, retryable = false, response = ResponseMessageConst.VOID)
    void commit(String transactionId, boolean onePhase);

    /**
     *
     * @param xid Java XA transaction id as defined in interface javax.transaction.xa.Xid.
     * @param timeout The timeout in seconds for XA operations such as prepare, commit, rollback.
     * @return The transaction unique identifier.
     */
    @Request(id = 5, retryable = false, response = ResponseMessageConst.STRING)
    Object create(Xid xid, long timeout);

    /**
     *
     * @param transactionId The id of the transaction to prepare.
     */
    @Request(id = 6, retryable = false, response = ResponseMessageConst.VOID)
    void prepare(String transactionId);

    /**
     *
     * @param transactionId The id of the transaction to rollback.
     */
    @Request(id = 7, retryable = false, response = ResponseMessageConst.VOID)
    void rollback(String transactionId);
}
