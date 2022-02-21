/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

@Generated("846f8102c4b4430aea6d290f7091c1b1")
public final class AnchorDataListHolderCodec {

    private AnchorDataListHolderCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.client.impl.protocol.codec.holder.AnchorDataListHolder anchorDataListHolder) {
        clientMessage.add(BEGIN_FRAME.copy());

        ListIntegerCodec.encode(clientMessage, anchorDataListHolder.getAnchorPageList());
        EntryListCodec.encode(clientMessage, anchorDataListHolder.getAnchorDataList(), DataCodec::encode, DataCodec::encodeNullable);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.client.impl.protocol.codec.holder.AnchorDataListHolder decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        java.util.List<java.lang.Integer> anchorPageList = ListIntegerCodec.decode(iterator);
        java.util.List<java.util.Map.Entry<com.hazelcast.internal.serialization.Data, com.hazelcast.internal.serialization.Data>> anchorDataList = EntryListCodec.decode(iterator, DataCodec::decode, DataCodec::decodeNullable);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.client.impl.protocol.codec.holder.AnchorDataListHolder(anchorPageList, anchorDataList);
    }
}
