/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.List;

public final class ClientMessageSplitter {

    private ClientMessageSplitter() {
    }

    /**
     * Splits a {@link ClientMessage} into frames of a maximum size.
     *
     * @param maxFrameSize  each split will have max size of maxFrameSize (last split can be smaller than frame size)
     * @param clientMessage main message that will be split
     * @return ordered array of split client message frames
     */
    public static List<ClientMessage> getSubFrames(int maxFrameSize, ClientMessage clientMessage) {
        int numberOFSubFrames = ClientMessageSplitter.getNumberOfSubFrames(maxFrameSize, clientMessage);
        ArrayList<ClientMessage> messages = new ArrayList<ClientMessage>(numberOFSubFrames);
        for (int i = 0; i < numberOFSubFrames; i++) {
            messages.add(ClientMessageSplitter.getSubFrame(maxFrameSize, i, numberOFSubFrames, clientMessage));
        }
        return messages;
    }

    static int getNumberOfSubFrames(int frameSize, ClientMessage originalClientMessage) {
        assert ClientMessage.HEADER_SIZE < frameSize;
        int frameLength = originalClientMessage.getFrameLength();
        int sizeWithoutHeader = frameSize - ClientMessage.HEADER_SIZE;
        System.out.println(ClientMessage.HEADER_SIZE);
        System.out.println(frameLength);
        System.out.println(sizeWithoutHeader);
        return (int) Math.ceil((float) (frameLength - ClientMessage.HEADER_SIZE) / sizeWithoutHeader);
    }

    static ClientMessage getSubFrame(int frameSize, int frameIndex, int numberOfSubFrames, ClientMessage originalClientMessage) {
        assert frameIndex > -1;
        assert frameIndex < numberOfSubFrames;
        assert ClientMessage.HEADER_SIZE < frameSize;

        int frameLength = originalClientMessage.getFrameLength();
        if (frameSize > frameLength) {
            assert frameIndex == 0;
            return originalClientMessage;
        }

        int subFrameMaxDataLength = frameSize - ClientMessage.HEADER_SIZE;
        int startOffset = ClientMessage.HEADER_SIZE + frameIndex * subFrameMaxDataLength;
        int subFrameDataLength = numberOfSubFrames != frameIndex + 1 ? subFrameMaxDataLength : frameLength - startOffset;
        ClientMessage subFrame = ClientMessage.createForEncode(ClientMessage.HEADER_SIZE + subFrameDataLength);
        System.arraycopy(originalClientMessage.buffer.byteArray(), startOffset,
                subFrame.buffer.byteArray(), subFrame.getDataOffset(), subFrameDataLength);

        if (frameIndex == 0) {
            subFrame.addFlag(ClientMessage.BEGIN_FLAG);
        } else if (numberOfSubFrames == frameIndex + 1) {
            subFrame.addFlag(ClientMessage.END_FLAG);
        }
        subFrame.setPartitionId(originalClientMessage.getPartitionId());
        subFrame.setFrameLength(ClientMessage.HEADER_SIZE + subFrameDataLength);
        subFrame.setMessageType(originalClientMessage.getMessageType());
        subFrame.setRetryable(originalClientMessage.isRetryable());
        subFrame.setCorrelationId(originalClientMessage.getCorrelationId());
        return subFrame;
    }
}
