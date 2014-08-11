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

package com.hazelcast.transaction.client;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.client.InvocationClientRequest;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.transaction.impl.RecoverTxnOperation;
import com.hazelcast.transaction.impl.RecoveredTransaction;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;
import com.hazelcast.util.ExceptionUtil;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.transaction.impl.TransactionManagerServiceImpl.RECOVER_TIMEOUT;

public class RecoverAllTransactionsRequest extends InvocationClientRequest {

    public RecoverAllTransactionsRequest() {
    }

    @Override
    public void invoke() {
        ClientEngine clientEngine = getClientEngine();
        ClusterService clusterService = clientEngine.getClusterService();
        Collection<MemberImpl> memberList = clusterService.getMemberList();
        TransactionManagerServiceImpl service = getService();

        List<Future<SerializableCollection>> futures = recoverTransactions(memberList);
        Set<Data> xids = new HashSet<Data>();
        for (Future<SerializableCollection> future : futures) {
            try {
                SerializableCollection collectionWrapper = future.get(RECOVER_TIMEOUT, TimeUnit.MILLISECONDS);
                for (Data data : collectionWrapper) {
                    RecoveredTransaction rt = serializationService.toObject(data);
                    service.addClientRecoveredTransaction(rt);
                    xids.add(serializationService.toData(rt.getXid()));
                }
            } catch (MemberLeftException e) {
                ILogger logger = clientEngine.getLogger(RecoverAllTransactionsRequest.class);
                logger.warning("Member left while recovering: " + e);
            } catch (Throwable e) {
                handleException(clientEngine, e);
            }
        }
        ClientEndpoint endpoint = getEndpoint();
        endpoint.sendResponse(new SerializableCollection(xids), getCallId());
    }

    private List<Future<SerializableCollection>> recoverTransactions(Collection<MemberImpl> memberList) {
        List<Future<SerializableCollection>> futures = new ArrayList<Future<SerializableCollection>>(memberList.size());
        for (MemberImpl member : memberList) {
            RecoverTxnOperation op = new RecoverTxnOperation();
            Future<SerializableCollection> f = createInvocationBuilder(TransactionManagerServiceImpl.SERVICE_NAME,
                    op, member.getAddress()).invoke();
            futures.add(f);
        }
        return futures;
    }

    private void handleException(ClientEngine clientEngine, Throwable e) {
        Throwable cause = getCause(e);
        if (cause instanceof TargetNotMemberException) {
            ILogger logger = clientEngine.getLogger(RecoverAllTransactionsRequest.class);
            logger.warning("Member left while recovering: " + cause);
        } else {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private Throwable getCause(Throwable e) {
        if (e instanceof ExecutionException) {
            if (e.getCause() != null) {
                return e.getCause();
            }
        }
        return e;
    }

    @Deprecated
    public String getServiceName() {
        return TransactionManagerServiceImpl.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ClientTxnPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClientTxnPortableHook.RECOVER_ALL;
    }

    @Override
    public Permission getRequiredPermission() {
        return new TransactionPermission();
    }
}
