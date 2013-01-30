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

package com.hazelcast.semaphore;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;

/**
 * @ali 1/23/13
 */
public class DeadMemberOperation extends SemaphoreBackupAwareOperation implements Notifier {

    Address firstCaller;

    public DeadMemberOperation() {
    }

    public DeadMemberOperation(String name, Address firstCaller) {
        super(name, -1);
        this.firstCaller = firstCaller;
    }

    public void run() throws Exception {
        response = getPermit().memberRemoved(firstCaller);
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    public boolean returnsResponse() {
        return false;
    }

    public Operation getBackupOperation() {
        return new DeadMemberBackupOperation(name, firstCaller);
    }

    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        firstCaller.writeData(out);
    }

    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        firstCaller = new Address();
        firstCaller.readData(in);
    }

    public boolean shouldNotify() {
        return Boolean.TRUE.equals(response);
    }

    public WaitNotifyKey getNotifiedKey() {
        return new SemaphoreWaitNotifyKey(name, "acquire");
    }
}
