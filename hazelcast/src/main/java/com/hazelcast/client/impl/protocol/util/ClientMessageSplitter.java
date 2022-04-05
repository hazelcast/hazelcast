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

package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.spi.impl.sequence.CallIdSequenceWithoutBackpressure;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.client.impl.protocol.ClientMessage.BEGIN_FRAGMENT_FLAG;
import static com.hazelcast.client.impl.protocol.ClientMessage.END_FRAGMENT_FLAG;

public final class ClientMessageSplitter {

    private static final CallIdSequenceWithoutBackpressure FRAGMENT_ID_SEQUENCE = new CallIdSequenceWithoutBackpressure();

    private enum ReadState {
        //means last fragment is added to client message, need to create new fragment
        BEGINNING,
        //means at least a frame is added to current fragment
        MIDDLE
    }

    private ClientMessageSplitter() {
    }

    /**
     * Splits a {@link ClientMessage} into fragments of a maximum size.
     *
     * @param maxFrameSize  each split will have max size of maxFrameSize
     * @param clientMessage main message that will be split
     * @return ordered array of split client message frames
     */
    public static List<ClientMessage> getFragments(int maxFrameSize, ClientMessage clientMessage) {
        if (clientMessage.getFrameLength() <= maxFrameSize) {
            return Collections.singletonList(clientMessage);
        }
        long fragmentId = FRAGMENT_ID_SEQUENCE.next();
        LinkedList<ClientMessage> fragments = new LinkedList<>();
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();

        ReadState state = ReadState.BEGINNING;
        int length = 0;
        ClientMessage fragment = null;
        while (iterator.hasNext()) {
            ClientMessage.Frame frame = iterator.peekNext();
            int frameSize = frame.getSize();
            length += frameSize;

            if (frameSize > maxFrameSize) {
                iterator.next();
                if (state == ReadState.MIDDLE) {
                    fragments.add(fragment);
                }
                fragment = createFragment(fragmentId);
                fragment.add(frame.copy());
                fragments.add(fragment);
                state = ReadState.BEGINNING;
                length = 0;
            } else if (length <= maxFrameSize) {
                iterator.next();
                if (state == ReadState.BEGINNING) {
                    fragment = createFragment(fragmentId);
                }
                fragment.add(frame.copy());
                state = ReadState.MIDDLE;
            } else {
                assert state == ReadState.MIDDLE;
                fragments.add(fragment);
                state = ReadState.BEGINNING;
                length = 0;
            }
        }
        if (state == ReadState.MIDDLE) {
            fragments.add(fragment);
        }
        fragments.getFirst().getStartFrame().flags |= BEGIN_FRAGMENT_FLAG;
        fragments.getLast().getStartFrame().flags |= END_FRAGMENT_FLAG;
        return fragments;
    }

    private static ClientMessage createFragment(long fragmentId) {
        ClientMessage fragment;
        fragment = ClientMessage.createForEncode();
        ClientMessage.Frame frame = new ClientMessage.Frame(new byte[Bits.LONG_SIZE_IN_BYTES]);
        Bits.writeLongL(frame.content, ClientMessage.FRAGMENTATION_ID_OFFSET, fragmentId);
        fragment.add(frame);
        return fragment;
    }
}
