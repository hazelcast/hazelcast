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

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

/**
 * Causes the current thread to wait until it is signalled or Thread#interrupt interrupted. The lock associated with
 * this Condition is atomically released and the current thread becomes disabled for thread scheduling purposes and
 * lies dormant until one of four things happens:
 * Some other thread invokes the #signal method for this Condition and the current thread happens to be chosen as the
 * thread to be awakened; or Some other thread invokes the #signalAll method for this Condition; or Some other thread
 * Thread#interrupt interrupts the current thread, and interruption of thread suspension is supported; or A spurious wakeup occurs.
 * In all cases, before this method can return the current thread must re-acquire the lock associated with this condition.
 * When the thread returns it is guaranteed to hold this lock. If the current thread: has its interrupted status set
 * on entry to this method; or is Thread#interrupt interrupted while waiting and interruption of thread suspension
 * is supported, then INTERRUPTED is thrown and the current thread's interrupted status is cleared. It is
 * not specified, in the first case, whether or not the test for interruption occurs before the lock is released.
 * The current thread is assumed to hold the lock associated with this Condition when this method is called.
 * It is up to the implementation to determine if this is the case and if not, how to respond. Typically, an exception will be
 * thrown (such as ILLEGAL_MONITOR_STATE) and the implementation must document that fact.
 * An implementation can favor responding to an interrupt over normal method return in response to a signal. In that
 * case the implementation must ensure that the signal is redirected to another waiting thread, if there is one.
 */
public final class ConditionBeforeAwaitCodec {
    //hex: 0x080200
    public static final int REQUEST_MESSAGE_TYPE = 524800;
    //hex: 0x080201
    public static final int RESPONSE_MESSAGE_TYPE = 524801;
    private static final int REQUEST_THREAD_ID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_REFERENCE_ID_FIELD_OFFSET = REQUEST_THREAD_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_REFERENCE_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = CORRELATION_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private ConditionBeforeAwaitCodec() {
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class RequestParameters {

        /**
         * Name of the Condition
         */
        public java.lang.String name;

        /**
         * The id of the user thread performing the operation. It is used to guarantee that only the lock holder thread (if a lock exists on the entry) can perform the requested operation.
         */
        public long threadId;

        /**
         * Name of the lock to wait on.
         */
        public java.lang.String lockName;

        /**
         * The client-wide unique id for this request. It is used to make the request idempotent by sending the same reference id during retries.
         */
        public long referenceId;
    }

    public static ClientMessage encodeRequest(java.lang.String name, long threadId, java.lang.String lockName, long referenceId) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("Condition.BeforeAwait");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeLong(initialFrame.content, REQUEST_THREAD_ID_FIELD_OFFSET, threadId);
        encodeLong(initialFrame.content, REQUEST_REFERENCE_ID_FIELD_OFFSET, referenceId);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, name);
        StringCodec.encode(clientMessage, lockName);
        return clientMessage;
    }

    public static ConditionBeforeAwaitCodec.RequestParameters decodeRequest(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.threadId = decodeLong(initialFrame.content, REQUEST_THREAD_ID_FIELD_OFFSET);
        request.referenceId = decodeLong(initialFrame.content, REQUEST_REFERENCE_ID_FIELD_OFFSET);
        request.name = StringCodec.decode(iterator);
        request.lockName = StringCodec.decode(iterator);
        return request;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public static class ResponseParameters {
    }

    public static ClientMessage encodeResponse() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        return clientMessage;
    }

    public static ConditionBeforeAwaitCodec.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        ListIterator<ClientMessage.Frame> iterator = clientMessage.listIterator();
        ResponseParameters response = new ResponseParameters();
        //empty initial frame
        iterator.next();
        return response;
    }

}
