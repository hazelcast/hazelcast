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

package com.hazelcast.client.impl.protocol.task.transaction;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.TransactionRecoverAllParameters;
import com.hazelcast.client.impl.protocol.parameters.TransactionRecoverAllResultParameters;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.cluster.ClusterService;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.SerializableCollection;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.transaction.impl.RecoverTxnOperation;
import com.hazelcast.transaction.impl.RecoveredTransaction;
import com.hazelcast.transaction.impl.SerializableXID;
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


public class TransactionRecoverAllMessageTask
        extends AbstractCallableMessageTask<TransactionRecoverAllParameters> {

    private final ILogger logger;

    public TransactionRecoverAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        logger = clientEngine.getLogger(TransactionRecoverAllMessageTask.class);
    }

    @Override
    protected TransactionRecoverAllParameters decodeClientMessage(ClientMessage clientMessage) {
        return TransactionRecoverAllParameters.decode(clientMessage);
    }

    @Override
    public String getServiceName() {
        return TransactionManagerServiceImpl.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new TransactionPermission();
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return null;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    protected ClientMessage call() throws Exception {
        ClusterService clusterService = clientEngine.getClusterService();
        Collection<MemberImpl> memberList = clusterService.getMemberList();
        TransactionManagerServiceImpl service = getService(getServiceName());

        List<Future<SerializableCollection>> futures = recoverTransactions(memberList);
        Set<SerializableXID> xids = new HashSet<SerializableXID>();
        for (Future<SerializableCollection> future : futures) {
            try {
                SerializableCollection collectionWrapper = future.get(RECOVER_TIMEOUT, TimeUnit.MILLISECONDS);
                for (Data data : collectionWrapper) {
                    RecoveredTransaction rt = serializationService.toObject(data);
                    service.addClientRecoveredTransaction(rt);
                    xids.add(rt.getXid());
                }
            } catch (MemberLeftException e) {
                logger.warning("Member left while recovering: " + e);
            } catch (Throwable e) {
                handleException(e);
            }
        }
        return TransactionRecoverAllResultParameters.encode(xids);
    }

    private List<Future<SerializableCollection>> recoverTransactions(Collection<MemberImpl> memberList) {
        List<Future<SerializableCollection>> futures = new ArrayList<Future<SerializableCollection>>(memberList.size());
        InternalOperationService operationService = nodeEngine.getOperationService();
        for (MemberImpl member : memberList) {
            RecoverTxnOperation op = new RecoverTxnOperation();
            Address address = member.getAddress();
            Future<SerializableCollection> f =
                    operationService.createInvocationBuilder(getServiceName(), op, address).invoke();
            futures.add(f);
        }
        return futures;
    }

    private void handleException(Throwable e) {
        Throwable cause = getCause(e);
        if (cause instanceof TargetNotMemberException) {
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
}
