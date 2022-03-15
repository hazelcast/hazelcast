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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.nio.ascii.MemcacheTextDecoder;
import com.hazelcast.internal.nio.ascii.RestApiTextDecoder;

import static com.hazelcast.internal.util.JVMUtil.upcast;

import java.nio.ByteBuffer;

public class TextHandshakeDecoder
        extends SingleProtocolDecoder {

    public TextHandshakeDecoder(ProtocolType supportedProtocol, InboundHandler next, SingleProtocolEncoder encoder) {
        super(supportedProtocol, next, encoder);
    }

    @Override
    protected boolean verifyProtocol(String incomingProtocol) {
        super.verifyProtocolCalled = true;
        handleUnexpectedProtocol(incomingProtocol);
        if (ProtocolType.REST.equals(supportedProtocol)) {
            if (!RestApiTextDecoder.TEXT_PARSERS.isCommandPrefix(incomingProtocol)) {
                encoder.signalWrongProtocol(
                        "Unsupported protocol exchange detected, expected protocol: REST"
                                + ", actual protocol or first three bytes are: " + incomingProtocol);
                return false;
            }
        } else {
            if (!MemcacheTextDecoder.TEXT_PARSERS.isCommandPrefix(incomingProtocol)) {
                encoder.signalWrongProtocol(
                        "Unsupported protocol exchange detected, expected protocol: MEMCACHED"
                                + ", actual protocol or first three bytes are: " + incomingProtocol);
                return false;
            }
        }
        return true;
    }

    @Override
    protected void setupNextDecoder() {
        super.setupNextDecoder();
        // we need to restore whatever is read
        ByteBuffer src = this.src;
        ByteBuffer dst = (ByteBuffer) inboundHandlers[0].src();
        upcast(src).flip();
        dst.put(src);
    }
}
