/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.queue.client;

import com.hazelcast.client.InvocationClientRequest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.queue.QueuePortableHook;
import com.hazelcast.queue.QueueService;

import java.io.IOException;

/**
 * @ali 6/5/13
 */
public class TxnOfferRequest extends InvocationClientRequest implements Portable{

    String name;
    long timeout;
    Data data;

    public TxnOfferRequest() {
    }

    public TxnOfferRequest(String name, long timeout, Data data) {
        this.name = name;
        this.timeout = timeout;
        this.data = data;
    }

    protected void invoke() {
//        TxnReserveOfferOperation operation = new TxnReserveOfferOperation(name, timeout, offeredQueue.size());
//        try {
//            Invocation invocation = getNodeEngine().getOperationService().createInvocationBuilder(QueueService.SERVICE_NAME, operation, partitionId).build();
//            Future<Long> f = invocation.invoke();
//            Long itemId = f.get();
//            if (itemId != null) {
//                if (!itemIdSet.add(itemId)) {
//                    throw new TransactionException("Duplicate itemId: " + itemId);
//                }
//                offeredQueue.offer(new QueueItem(null, itemId, data));
//                tx.addTransactionLog(new QueueTransactionLog(itemId, name, partitionId, new TxnOfferOperation(name, itemId, data)));
//                return true;
//            }
//        } catch (Throwable t) {
//            throw ExceptionUtil.rethrow(t);
//        }
    }

    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    public int getFactoryId() {
        return QueuePortableHook.F_ID;
    }

    public int getClassId() {
        return QueuePortableHook.F_ID;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n",name);
        writer.writeLong("t",timeout);
        data.writeData(writer.getRawDataOutput());
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        timeout = reader.readLong("t");
        data = new Data();
        data.readData(reader.getRawDataInput());
    }
}
