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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;

// This thing sucks because it is specific to io uring even though it should be something for all
// eventloop implementations.
public class StorageDeviceRegistry {
    private final List<StorageDeviceScheduler> devs = new ArrayList<>();
    private IOUringEventloop eventloop;

    public StorageDeviceRegistry() {
    }

    void init(IOUringEventloop eventloop) {
        this.eventloop = eventloop;

        // nasty hack
        // currently we do not have a nice mechanism to register the drives.
        registerStorageDevice(System.getProperty("user.home"), 512, 512);
    }

    StorageDeviceScheduler findStorageDevice(String path) {
        for (StorageDeviceScheduler dev : devs) {
            if (path.startsWith(dev.path)) {
                return dev;
            }
        }
        return null;
    }

    /**
     * This method should be called before the StorageScheduler is being used.
     * <p>
     * This method is not thread-safe.
     *
     * @param path          the path to the storage device.
     * @param maxConcurrent the maximum number of concurrent requests for the device.
     * @param maxPending    the maximum number of request that can be buffered
     */
    public void registerStorageDevice(String path, int maxConcurrent, int maxPending) {
        File file = new File(path);
        if (!file.exists()) {
            throw new RuntimeException("A storage device [" + path + "] doesn't exit");
        }

        if (!file.isDirectory()) {
            throw new RuntimeException("A storage device [" + path + "] is not a directory");
        }

        if (findStorageDevice(path) != null) {
            throw new RuntimeException("A storage device with path [" + path + "] already exists");
        }

        checkPositive(maxConcurrent, "maxConcurrent");

        StorageDeviceScheduler storageDeviceScheduler = new StorageDeviceScheduler(path, maxConcurrent, maxPending, eventloop);
        devs.add(storageDeviceScheduler);
    }
}
