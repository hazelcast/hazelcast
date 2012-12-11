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

import com.hazelcast.nio.Data;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitSupport;

/**
 * User: ali
 * Date: 11/14/12
 * Time: 12:14 AM
 */
public class OfferOperation extends QueueDataOperation implements WaitSupport, Notifier {

    public OfferOperation() {
    }

    public OfferOperation(final String name, final Data data) {
        super(name, data);
    }

    public void run() {
        response = container.dataQueue.offer(data);
    }

    public Operation getBackupOperation() {
        return new QueueBackupOperation(new OfferOperation(name, data));
    }

    public Object getNotifiedKey() {
        return getName() + ":take";
    }

    public Object getWaitKey() {
        return getName() + ":offer";
    }

    public boolean shouldWait() {
//        return container.dataQueue.size() >= Queue.MaxSize;
        return false;
    }

    public long getWaitTimeoutMillis() {
        return 0;
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(Boolean.FALSE);
    }
}
