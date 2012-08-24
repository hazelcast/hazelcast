/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.map;

import com.hazelcast.core.Transaction;
import com.hazelcast.impl.ThreadContext;
import com.hazelcast.impl.TransactionImpl;
import com.hazelcast.impl.spi.Invocation;
import com.hazelcast.impl.spi.NodeService;
import com.hazelcast.impl.spi.Response;
import com.hazelcast.impl.spi.RetryableException;
import com.hazelcast.nio.Data;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.impl.map.MapService.MAP_SERVICE_NAME;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class MapProxy {
    final NodeService nodeService;
    final MapService mapService;

    public MapProxy(NodeService nodeService) {
        this.nodeService = nodeService;
        this.mapService = nodeService.getService(MAP_SERVICE_NAME);
    }

    public Object put(String name, Object k, Object v, long ttl) {
        ThreadContext threadContext = ThreadContext.get();
        threadContext.setCurrentInstance(nodeService.getNode().hazelcastInstance);
        Data key = toData(k);
        int partitionId = nodeService.getPartitionId(key);
        TransactionImpl txn = threadContext.getTransaction();
        String txnId = null;
        if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
            txnId = txn.getTxnId();
            txn.attachParticipant(MAP_SERVICE_NAME, partitionId);
        }
        PutOperation putOperation = new PutOperation(name, toData(k), v, txnId, ttl);
        long backupCallId = mapService.createNewBackupCallQueue();
//        System.out.println(nodeService.getThisAddress() + " map.put() with BACKUP ID " + backupCallId);
        try {
            putOperation.setBackupCallId(backupCallId);
            putOperation.setServiceName(MAP_SERVICE_NAME);
            Invocation invocation = nodeService.createSingleInvocation(MAP_SERVICE_NAME, putOperation, partitionId).build();
            Future f = invocation.invoke();
            Object response = f.get();
            BlockingQueue backupResponses = mapService.getBackupCallQueue(backupCallId);
            Object backupResponse = backupResponses.poll(10, TimeUnit.SECONDS);
//            if (backupResponse == null) {
//                System.out.println(nodeService.getThisAddress() + " Has Null backup " + backupCallId);
//            }
            Object returnObj = null;
            if (response instanceof Response) {
                Response r = (Response) response;
                returnObj = r.getResult();
            } else {
                returnObj = toObject(response);
            }
            if (returnObj instanceof Throwable) {
                throw (Throwable) returnObj;
            }
            return returnObj;
        } catch (RetryableException t) {
            t.printStackTrace();
            throw t;
        } catch (Throwable throwable) {
            throwable.printStackTrace();
            throw new RuntimeException(throwable);
        } finally {
            mapService.removeBackupCallQueue(backupCallId);
        }
    }

    public Object put2(String name, Object k, Object v, long ttl) {
        Data key = toData(k);
        int partitionId = nodeService.getPartitionId(key);
        ThreadContext threadContext = ThreadContext.get();
        TransactionImpl txn = threadContext.getTransaction();
        String txnId = null;
        if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
            txnId = txn.getTxnId();
            txn.attachParticipant(MAP_SERVICE_NAME, partitionId);
        }
        PutOperation putOperation = new PutOperation(name, toData(k), v, txnId, ttl);
        try {
            Invocation invocation = nodeService.createSingleInvocation(MAP_SERVICE_NAME, putOperation, partitionId).build();
            Future f = invocation.invoke();
            Object response = f.get();
            if (response instanceof Response) {
                return ((Response) response).getResult();
            }
            return toObject((Data) response);
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public Object getOperation(String name, Object k) {
        ThreadContext threadContext = ThreadContext.get();
        threadContext.setCurrentInstance(nodeService.getNode().hazelcastInstance);
        Data key = toData(k);
        int partitionId = nodeService.getPartitionId(key);
        GetOperation getOperation = new GetOperation(name, toData(k));
        getOperation.setServiceName(MAP_SERVICE_NAME);
        try {
            Invocation invocation = nodeService.createSingleInvocation(MAP_SERVICE_NAME, getOperation, partitionId).build();
            Future f = invocation.invoke();
            Data response = (Data) f.get();
            return toObject(response);
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public int getSize(String name) {
        try {
            Map<Integer, Object> results = nodeService.invokeOnAllPartitions(MAP_SERVICE_NAME, new MapSizeOperation(name));
            int total = 0;
            for (Object result : results.values()) {
                Integer size = (Integer) result;
                System.out.println(">> " + size);
                total += size;
            }
            return total;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}
