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

package com.hazelcast.client.executor;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * @author ali 5/27/13
 */
public class CallableTask implements Callable<String>, DataSerializable {

    private String param;

    public CallableTask() {
    }

    public CallableTask(String param) {
        this.param = param;
    }

    public String call() throws Exception {
        return param + ":result";
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(param);
    }

    public void readData(ObjectDataInput in) throws IOException {
        param = in.readUTF();
    }
}
