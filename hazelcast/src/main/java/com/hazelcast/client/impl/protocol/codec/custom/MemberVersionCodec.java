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

@Generated("a20adaebb8d3ac896bef0c9dad3e84c9")
public final class MemberVersionCodec {
    private static final int MAJOR_FIELD_OFFSET = 0;
    private static final int MINOR_FIELD_OFFSET = MAJOR_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int PATCH_FIELD_OFFSET = MINOR_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = PATCH_FIELD_OFFSET + INT_SIZE_IN_BYTES;

    private MemberVersionCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.version.MemberVersion memberVersion) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeInt(initialFrame.content, MAJOR_FIELD_OFFSET, memberVersion.getMajor());
        encodeInt(initialFrame.content, MINOR_FIELD_OFFSET, memberVersion.getMinor());
        encodeInt(initialFrame.content, PATCH_FIELD_OFFSET, memberVersion.getPatch());
        clientMessage.add(initialFrame);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.version.MemberVersion decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        int major = decodeInt(initialFrame.content, MAJOR_FIELD_OFFSET);
        int minor = decodeInt(initialFrame.content, MINOR_FIELD_OFFSET);
        int patch = decodeInt(initialFrame.content, PATCH_FIELD_OFFSET);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.version.MemberVersion(major, minor, patch);
    }
}
