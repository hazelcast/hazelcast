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

package com.hazelcast.semaphore;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

/**
 * @ali 1/22/13
 */
public abstract class SemaphoreBackupOperation extends SemaphoreOperation implements BackupOperation {

    Address firstCaller;

    protected SemaphoreBackupOperation() {
    }

    protected SemaphoreBackupOperation(String name, int permitCount, Address firstCaller) {
        super(name, permitCount);
        this.firstCaller = firstCaller;
    }

    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        if (firstCaller == null){
            out.writeBoolean(false);
        }
        else {
            out.writeBoolean(true);
            firstCaller.writeData(out);
        }
    }

    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        if (in.readBoolean()){
            firstCaller = new Address();
            firstCaller.readData(in);
        }
    }
}
