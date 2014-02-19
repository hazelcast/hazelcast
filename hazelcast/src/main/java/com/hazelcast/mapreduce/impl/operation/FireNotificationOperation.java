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

package com.hazelcast.mapreduce.impl.operation;

import com.hazelcast.mapreduce.impl.MapReduceDataSerializerHook;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.notification.MapReduceNotification;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * This operation is used to transport and execute a notification on a remote node since
 * the current event service is not capable of reliable transmission so this is a fallback
 * to sync operations which is hopefully only a temporary workaround!
 */
public class FireNotificationOperation
        extends ProcessingOperation {

    private MapReduceNotification notification;

    public FireNotificationOperation() {
    }

    public FireNotificationOperation(MapReduceNotification notification) {
        super(notification.getName(), notification.getJobId());
        this.notification = notification;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public void run()
            throws Exception {
        MapReduceService mapReduceService = getService();
        mapReduceService.dispatchEvent(notification);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(notification);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        notification = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.FIRE_NOTIFICATION_OPERATION;
    }

}
