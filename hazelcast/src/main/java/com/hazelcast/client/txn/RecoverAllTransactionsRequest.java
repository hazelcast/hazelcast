/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.txn;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.InvocationClientRequest;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.transaction.impl.RecoverTxnOperation;
import com.hazelcast.transaction.impl.RecoveredTransaction;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;
import com.hazelcast.util.ExceptionUtil;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.transaction.impl.TransactionManagerServiceImpl.RECOVER_TIMEOUT;

/**
 * @author ali 14/02/14
 */
public class RecoverAllTransactionsRequest extends InvocationClientRequest implements Portable {

    public RecoverAllTransactionsRequest() {
    }

    @Override
    public void invoke() {
        final ClientEngine clientEngine = getClientEngine();
        final ClusterService clusterService = clientEngine.getClusterService();
        final Collection<MemberImpl> memberList = clusterService.getMemberList();
        final TransactionManagerServiceImpl service = getService();

        List<Future<SerializableCollection>> futures = new ArrayList<Future<SerializableCollection>>(memberList.size());
        for (MemberImpl member : memberList) {
            final Future<SerializableCollection> f = createInvocationBuilder(TransactionManagerServiceImpl.SERVICE_NAME,
                    new RecoverTxnOperation(), member.getAddress()).invoke();
            futures.add(f);
        }
        Set<Data> xids = new HashSet<Data>();
        for (Future<SerializableCollection> future : futures) {
            try {
                final SerializableCollection collectionWrapper = future.get(RECOVER_TIMEOUT, TimeUnit.MILLISECONDS);
                for (Data data : collectionWrapper) {
                    final RecoveredTransaction rt = (RecoveredTransaction)clientEngine.toObject(data);
                    service.addClientRecoveredTransaction(rt);
                    xids.add(clientEngine.toData(rt.getXid()));
                }
            } catch (MemberLeftException e) {
                final ILogger logger = clientEngine.getLogger(RecoverAllTransactionsRequest.class);
                logger.warning("Member left while recovering: " + e);
            } catch (Throwable e) {
                if (e instanceof ExecutionException) {
                    e = e.getCause() != null ? e.getCause() : e;
                }
                if (e instanceof TargetNotMemberException) {
                    final ILogger logger = clientEngine.getLogger(RecoverAllTransactionsRequest.class);
                    logger.warning("Member left while recovering: " + e);
                } else {
                    throw ExceptionUtil.rethrow(e);
                }
            }
        }
        final ClientEndpoint endpoint = getEndpoint();
        endpoint.sendResponse(new SerializableCollection(xids), getCallId());
    }

    public String getServiceName() {
        return TransactionManagerServiceImpl.SERVICE_NAME;
    }

    public int getFactoryId() {
        return ClientTxnPortableHook.F_ID;
    }

    public int getClassId() {
        return ClientTxnPortableHook.RECOVER_ALL;
    }

}
