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

package com.hazelcast.client.txn;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * @author ali 02/01/14
 */
public abstract class BaseTransactionRequest extends CallableClientRequest implements Portable {

    protected String txnId;

    public BaseTransactionRequest() {
    }

    public void setTxnId(String txnId) {
        this.txnId = txnId;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("tId", txnId);
    }

    public void read(PortableReader reader) throws IOException {
        txnId = reader.readUTF("tId");
    }
}
