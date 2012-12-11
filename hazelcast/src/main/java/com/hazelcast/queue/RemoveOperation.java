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
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.Operation;

/**
 * @ali 12/6/12
 */
public class RemoveOperation extends QueueDataOperation {

    public RemoveOperation() {
    }

    public RemoveOperation(String name, Data data) {
        super(name, data);
    }

    public void run() throws Exception {
        response = container.dataQueue.remove(data);
    }

    public Operation getBackupOperation() {
        return new QueueBackupOperation(new RemoveOperation());
    }
}
