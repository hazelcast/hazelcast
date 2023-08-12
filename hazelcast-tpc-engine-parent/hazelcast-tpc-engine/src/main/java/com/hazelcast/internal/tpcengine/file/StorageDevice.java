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

/**
 * Represents some a block device like an NVMe based SSD.
 * <p/>
 * One of the problems is that every eventloop will assume full ownership of this device.
 * So if you have maxConcurrency is 100 and 10 reactors, the total concurrency is 10x100=1000.
 */
public class StorageDevice {
    private final int maxConcurrent;
    private final String path;
    private final int maxWaiting;

    /**
     * @param path            the path of the storage device.
     * @param concurrentLimit the number of concurrent requests on the dev.
     * @param maxWaiting      the maximum number of waiting in case of no space for waiting requests.
     */
    public StorageDevice(String path,
                         int concurrentLimit,
                         int maxWaiting) {
        this.path = path;
        File file = new File(path);
        if (!file.isDirectory()) {
            throw new IllegalArgumentException("Path [" + path + "] is not a valid directory.");
        }
        this.maxConcurrent = concurrentLimit;
        this.maxWaiting = maxWaiting;
    }

    public int concurrentLimit() {
        return maxConcurrent;
    }

    public String path() {
        return path;
    }

    public int maxWaiting() {
        return maxWaiting;
    }

    @Override
    public String toString() {
        return "StorageDevice[" + path + "]";
    }
}
