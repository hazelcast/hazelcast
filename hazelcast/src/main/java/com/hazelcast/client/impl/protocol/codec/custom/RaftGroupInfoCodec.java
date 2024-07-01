/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

@SuppressWarnings("unused")
@Generated("fba73a8528fdffb9084626ab20fb6411")
public final class RaftGroupInfoCodec {

    private RaftGroupInfoCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.cp.internal.RaftGroupInfo raftGroupInfo) {
        clientMessage.add(BEGIN_FRAME.copy());

        RaftGroupIdCodec.encode(clientMessage, raftGroupInfo.getGroupId());
        CPMemberCodec.encode(clientMessage, raftGroupInfo.getLeader());
        ListMultiFrameCodec.encode(clientMessage, raftGroupInfo.getFollowers(), CPMemberCodec::encode);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.cp.internal.RaftGroupInfo decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        com.hazelcast.cp.internal.RaftGroupId groupId = RaftGroupIdCodec.decode(iterator);
        com.hazelcast.cp.internal.CPMemberInfo leader = CPMemberCodec.decode(iterator);
        java.util.Collection<com.hazelcast.cp.CPMember> followers = ListMultiFrameCodec.decode(iterator, CPMemberCodec::decode);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.cp.internal.RaftGroupInfo(groupId, leader, followers);
    }
}
