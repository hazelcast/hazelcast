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

package com.hazelcast.queue;

import com.hazelcast.config.QueueConfig;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.spi.Invocation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * User: ali
 * Date: 11/22/12
 * Time: 11:00 AM
 */
public class QueueContainer implements DataSerializable {

    private final Queue<QueueItem> itemQueue = new LinkedList<QueueItem>();

    int partitionId;

    QueueConfig config;

    String name;

    QueueService queueService;

    public QueueContainer(){
    }

    public QueueContainer(QueueService queueService, int partitionId, QueueConfig config, String name) {
        this.queueService = queueService;
        this.partitionId = partitionId;
        this.config = config;
        this.name = name;
    }

    public boolean offer(Data data){
        QueueItem item = new QueueItem(data);
        return itemQueue.offer(item);
    }

    public int size(){
        return itemQueue.size();
    }

    public void clear(){
        itemQueue.clear(); //TODO how about remove event
    }

    public Data poll(){
        QueueItem item = itemQueue.poll();
        return item == null ? null : item.data;
    }

    public boolean remove(Data data){
        QueueItem item = new QueueItem(data);
        return itemQueue.remove(item);
    }

    public Data peek(){
        QueueItem item = itemQueue.peek();
        return item == null ? null : item.data;
    }

    public boolean contains(Set<Data> dataSet){
        return itemQueue.containsAll(dataSet);
    }

    public void writeData(DataOutput out) throws IOException {    //TODO listeners
        out.writeInt(partitionId);
        out.writeUTF(name);
        out.writeInt(itemQueue.size());
        Iterator<QueueItem> iterator = itemQueue.iterator();
        while (iterator.hasNext()) {
            QueueItem item = iterator.next();
            item.data.writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        partitionId = in.readInt();
        name = in.readUTF();
        int size = in.readInt();
        for (int j = 0; j < size; j++) {
            Data data = new Data();
            data.readData(in);
            QueueItem item = new QueueItem(data);
            itemQueue.offer(item);
        }
    }

}

