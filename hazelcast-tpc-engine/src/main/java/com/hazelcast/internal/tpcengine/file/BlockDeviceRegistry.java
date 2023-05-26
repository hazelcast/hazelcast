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

package com.hazelcast.internal.tpcengine.file;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

/**
 * A registry with all storage devices.
 * <p/>
 * This class isn't thread-safe.
 */
public class BlockDeviceRegistry {
    private final BlockDevice defaultDev = new BlockDevice("/", 128, 4096);
    private final List<BlockDevice> devs = new ArrayList<>();

    public BlockDevice findBlockDevice(String path) {
        checkNotNull(path, "path");

        for (BlockDevice dev : devs) {
            if (path.startsWith(dev.path())) {
                return dev;
            }
        }
        return defaultDev;
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
    public void register(String path, int maxConcurrent, int maxPending) {
        checkPositive(maxConcurrent, "maxConcurrent");
        checkPositive(maxPending, "maxPending");

        File dir = new File(path);
        if (!dir.exists()) {
            throw new RuntimeException("Storage device [" + path + "] doesn't exit.");
        }

        if (!dir.isDirectory()) {
            throw new RuntimeException("Storage device [" + path + "] is not a directory.");
        }

        if (dir.isHidden()) {
            throw new RuntimeException("Storage device [" + path + "] is hidden.");
        }

        if (!dir.canWrite()) {
            throw new RuntimeException("Storage device [" + path + "] is unwritable.");
        }

        if (!dir.canRead()) {
            throw new RuntimeException("Storage device [" + path + "] is unreadable.");
        }

        if (findBlockDevice(path) != defaultDev) {
            throw new RuntimeException("A storage device [" + path + "] already exists.");
        }

        BlockDevice dev = new BlockDevice(path, maxConcurrent, maxPending);
        devs.add(dev);
    }
}
