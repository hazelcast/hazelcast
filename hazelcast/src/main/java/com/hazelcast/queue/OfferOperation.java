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

import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.Data;
import com.hazelcast.spi.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

/**
 * User: ali
 * Date: 11/14/12
 * Time: 12:14 AM
 */
public class OfferOperation extends QueueTimedOperation implements WaitSupport, Notifier {

    private Data data;

    private transient QueueItem item;

    public OfferOperation() {
    }

    public OfferOperation(final String name, final long timeout, final Data data) {
        super(name, timeout);
        this.data = data;
    }

    public void run() {
        response = false;
        QueueContainer container = getContainer();
        item = container.offer(data);
        if (item != null) {
            response = true;
            if (!container.isStoreAsync()) {
                container.getStore().store(item.getItemId(), item.getData());
            }
        }
    }

    public void afterRun() throws Exception {
        if (Boolean.TRUE.equals(response)) {
            QueueContainer container = getContainer();
            publishEvent(ItemEventType.ADDED, data);
            if (container.isStoreAsync()) {
                container.getStore().store(item.getItemId(), item.getData());
            }
        }
    }

    public Operation getBackupOperation() {
        return new OfferBackupOperation(name, data);
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    public boolean shouldNotify() {
        //TODO
        return true;
    }

    public Object getNotifiedKey() {
        return getName() + ":poll";
    }

    public Object getWaitKey() {
        return getName() + ":offer";
    }

    public boolean shouldWait() {
        QueueContainer container = getContainer();
        return getWaitTimeoutMillis() != 0 && container.getConfig().getMaxSize() <= container.size();
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(Boolean.FALSE);
    }

    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        data.writeData(out);
    }

    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        data = new Data();
        data.readData(in);
    }
}
