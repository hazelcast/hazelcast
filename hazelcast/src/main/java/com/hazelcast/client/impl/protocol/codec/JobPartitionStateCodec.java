/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.annotation.Codec;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.mapreduce.JobPartitionState;
import com.hazelcast.mapreduce.impl.task.JobPartitionStateImpl;
import com.hazelcast.nio.Address;

@Codec(JobPartitionState.class)
public class JobPartitionStateCodec {

    private JobPartitionStateCodec() {
    }

    public static JobPartitionState decode(ClientMessage clientMessage) {
        final Address address = AddressCodec.decode(clientMessage);
        String state = clientMessage.getStringUtf8();

        return new JobPartitionStateImpl(address, JobPartitionState.State.valueOf(state));
    }

    public static void encode(JobPartitionState jobPartitionState, ClientMessage clientMessage) {
        AddressCodec.encode(jobPartitionState.getOwner(), clientMessage);
        clientMessage.set(jobPartitionState.getState().name());
    }

    public static int calculateDataSize(JobPartitionState jobPartitionState) {
        int dataSize = AddressCodec.calculateDataSize(jobPartitionState.getOwner());
        dataSize += ParameterUtil.calculateDataSize(jobPartitionState.getState().name());
        return dataSize;
    }
}
