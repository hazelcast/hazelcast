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

package com.hazelcast.internal.tpc.iouring;

import com.hazelcast.internal.tpc.ReactorBuilder;
import com.hazelcast.internal.tpc.ReactorType;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;

/**
 * Contains the configuration for the {@link IOUringReactor}.
 */
public class IOUringReactorBuilder extends ReactorBuilder {
    private static final int DEFAULT_IOURING_SIZE = 8192;

    int setupFlags;
    int entries = DEFAULT_IOURING_SIZE;
    StorageDeviceRegistry deviceRegistry = new StorageDeviceRegistry();
    boolean registerRing;

    public IOUringReactorBuilder() {
        super(ReactorType.IOURING);
    }

    @Override
    public IOUringReactor build() {
        return new IOUringReactor(this);
    }

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
     *
     * @param registerRingFd
     */
    public void setRegisterRingFd(boolean registerRingFd) {
        this.registerRing = registerRingFd;
    }

    /**
     * Sets the setup flags for the io_uring instance. See the IoUring.IORING_SETUP
     * constants.
     *
     * @param setupFlags the flags
     * @throws IllegalArgumentException if flags smaller than 0.
     */
    public void setSetupFlags(int setupFlags) {
        this.setupFlags = checkNotNegative(setupFlags, "setupFlags");
    }

    /**
     * Sets the number of entries for the io_uring instance.
     * <p/>
     * For more information see:
     * https://man7.org/linux/man-pages//man2/io_uring_enter.2.html
     *
     * @param entries the number of entries.
     * @throws IllegalArgumentException when entries smaller than 1.
     */
    public void setEntries(int entries) {
        this.entries = checkPositive(entries, "entries");
    }

    public void setStorageDeviceRegistry(StorageDeviceRegistry deviceRegistry) {
        this.deviceRegistry = checkNotNull(deviceRegistry);
    }
}
