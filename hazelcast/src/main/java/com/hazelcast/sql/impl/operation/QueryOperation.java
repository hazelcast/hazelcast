/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.sql.SqlService;

import java.io.IOException;

/**
 * Base class for query operations.
 */
// TODO: We do not execute these operation as normal operations. Instead, we serialize them to packets with special
//  flag. May be we do not need to extend the Operation at all, becuase only "callerId" field of that class is used.
public abstract class QueryOperation extends Operation {
    /** Epoch watermark which is used to piggyback on normal SQL message to propagate epoch watermark between nodes. */
    protected long epochWatermark;

    protected QueryOperation() {
        // No-op.
    }

    protected QueryOperation(long epochWatermark) {
        this.epochWatermark = epochWatermark;
    }

    public long getEpochWatermark() {
        return epochWatermark;
    }

    @Override
    public String getServiceName() {
        return SqlService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        throw new UnsupportedOperationException("Should not be called.");
    }

    @Override
    protected final void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeLong(epochWatermark);

        writeInternal0(out);
    }

    @Override
    protected final void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        epochWatermark = in.readLong();

        readInternal0(in);
    }

    protected abstract void writeInternal0(ObjectDataOutput out) throws IOException;
    protected abstract void readInternal0(ObjectDataInput in) throws IOException;
}
