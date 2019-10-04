/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.codec.custom;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

@Generated("c681c7f51c51e9519bd59e440f9820aa")
public final class ScheduledTaskHandlerCodec {
    private static final int PARTITION_ID_FIELD_OFFSET = 0;
    private static final int INITIAL_FRAME_SIZE = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private ScheduledTaskHandlerCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.scheduledexecutor.ScheduledTaskHandler scheduledTaskHandler) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, scheduledTaskHandler.getPartitionId());
        clientMessage.add(initialFrame);

        CodecUtil.encodeNullable(clientMessage, scheduledTaskHandler.getAddress(), AddressCodec::encode);
        StringCodec.encode(clientMessage, scheduledTaskHandler.getSchedulerName());
        StringCodec.encode(clientMessage, scheduledTaskHandler.getTaskName());

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.scheduledexecutor.impl.ScheduledTaskHandlerImpl decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int partitionId = decodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET);

        com.hazelcast.cluster.Address address = CodecUtil.decodeNullable(iterator, AddressCodec::decode);
        java.lang.String schedulerName = StringCodec.decode(iterator);
        java.lang.String taskName = StringCodec.decode(iterator);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.scheduledexecutor.impl.ScheduledTaskHandlerImpl(address, partitionId, schedulerName, taskName);
    }
}
