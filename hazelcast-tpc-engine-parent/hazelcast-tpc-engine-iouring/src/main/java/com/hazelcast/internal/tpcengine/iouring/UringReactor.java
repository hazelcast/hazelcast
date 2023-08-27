/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.iouring;


import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.Reactor;
import com.hazelcast.internal.tpcengine.ReactorType;
import com.hazelcast.internal.tpcengine.net.AbstractAsyncSocket;
import com.hazelcast.internal.tpcengine.net.AsyncServerSocket;
import com.hazelcast.internal.tpcengine.net.AsyncSocket;

import static com.hazelcast.internal.tpcengine.Reactor.State.RUNNING;
import static com.hazelcast.internal.tpcengine.iouring.Linux.errno;
import static com.hazelcast.internal.tpcengine.iouring.Linux.newSysCallFailedException;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNegative;

/**
 * io_uring {@link Reactor} implementation.
 *
 * <p>
 * Good read:
 * https://unixism.net/2020/04/io-uring-by-example-part-3-a-web-server-with-io-uring/
 * <p>
 * Another example (blocking socket)
 * https://github.com/ddeka0/AsyncIO/blob/master/src/asyncServer.cpp
 * <p>
 * no syscalls:
 * https://wjwh.eu/posts/2021-10-01-no-syscall-server-iouring.html
 */
public final class UringReactor extends Reactor {

    private final EventFd eventFd;
    private final UringEventloop eventloop;

    private UringReactor(Builder builder) {
        super(builder);
        this.eventloop = (UringEventloop) eventloop();
        this.eventFd = eventloop.eventFdHandler.eventFd;
    }

    @Override
    protected Eventloop newEventloop(Reactor.Builder reactorBuilder) {
        UringEventloop.Builder eventloopBuilder = new UringEventloop.Builder();
        eventloopBuilder.reactorBuilder = reactorBuilder;
        eventloopBuilder.reactor = this;
        return eventloopBuilder.build();
    }

    @Override
    public AsyncSocket.Builder newAsyncSocketBuilder() {
        checkRunning();

        UringAsyncSocket.Builder socketBuilder = new UringAsyncSocket.Builder(null);
        socketBuilder.networkScheduler = eventloop.networkScheduler();
        socketBuilder.reactor = this;
        socketBuilder.uring = eventloop.uring;
        return socketBuilder;
    }

    @Override
    public AsyncSocket.Builder newAsyncSocketBuilder(AbstractAsyncSocket.AcceptRequest acceptRequest) {
        checkRunning();

        UringAsyncSocket.Builder socketBuilder
                = new UringAsyncSocket.Builder((UringAsyncServerSocket.AcceptRequest) acceptRequest);
        socketBuilder.uring = eventloop.uring;
        socketBuilder.reactor = this;
        socketBuilder.networkScheduler = eventloop.networkScheduler();
        return socketBuilder;
    }

    @Override
    public AsyncServerSocket.Builder newAsyncServerSocketBuilder() {
        checkRunning();

        UringAsyncServerSocket.Builder serverSocketBuilder = new UringAsyncServerSocket.Builder();
        serverSocketBuilder.reactor = this;
        serverSocketBuilder.uring = eventloop.uring;
        serverSocketBuilder.networkScheduler = (UringNetworkScheduler) eventloop.networkScheduler();
        return serverSocketBuilder;
    }

    @Override
    public void wakeup() {
        if (spin || Thread.currentThread() == eventloopThread) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            int res = eventFd.write(1L);
            if (res == -1 && RUNNING.equals(state)) {
                throw newSysCallFailedException("Failed to write to eventfd.", "eventfd_write(2)", errno());
            }
        }
    }

    /**
     * An {@link UringReactor} builder.
     */
    @SuppressWarnings({"checkstyle:VisibilityModifier"})
    public static class Builder extends Reactor.Builder {

        /**
         * Sets the setup flags for the io_uring instance. See the IoUring.IORING_SETUP
         * constants.
         */
        public int setupFlags;

        /**
         * Configures if the file descriptor of the io_uring instance should be
         * registered. The purpose of registration it to speed up io_uring_enter.
         * <p/>
         * For more information see:
         * https://man7.org/linux/man-pages/man3/io_uring_register_ring_fd.3.html
         * <p/>
         * This is an ultra power feature and should probably not be used by anyone.
         * You can only have 16 io_uring instances with registered ring file
         * descriptor. If you create more, you will run into a 'Device or resource busy'
         * exception.
         */
        public boolean registerRing;

        public Builder() {
            super(ReactorType.IOURING);
        }

        @Override
        protected Reactor construct() {
            return new UringReactor(this);
        }

        @Override
        protected void conclude() {
            super.conclude();

            checkNotNegative(setupFlags, "setupFlags");
        }
    }
}

