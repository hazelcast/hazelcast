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

package com.hazelcast.internal.tpcengine.nio;

import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.file.AsyncFile;
import com.hazelcast.internal.tpcengine.file.StorageScheduler;
import com.hazelcast.internal.tpcengine.util.IntPromise;

import java.io.File;
import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;

public class NioAsyncFile extends AsyncFile {
    AsynchronousFileChannel channel;

    public NioAsyncFile(String path,
                        Eventloop eventloop,
                        StorageScheduler scheduler) {
        super(path, eventloop, scheduler);
    }

    @Override
    public long size() {
        try {
            return channel.size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IntPromise delete() {
        File file = new File(path());
        IntPromise promise = promiseAllocator.allocate();
        if (file.delete()) {
            promise.complete(0);
        } else {
            promise.completeExceptionally(new IOException("Failed to delete [" + path() + "]"));
        }
        return promise;
    }
}
