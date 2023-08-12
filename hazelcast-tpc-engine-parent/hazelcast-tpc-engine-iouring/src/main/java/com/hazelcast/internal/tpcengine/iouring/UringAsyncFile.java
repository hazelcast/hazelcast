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
import com.hazelcast.internal.tpcengine.file.AsyncFile;
import com.hazelcast.internal.tpcengine.file.StorageDevice;
import com.hazelcast.internal.tpcengine.file.StorageScheduler;
import com.hazelcast.internal.tpcengine.util.IntPromise;

/**
 * IOUring implementation of the {@link AsyncFile}.
 */
@SuppressWarnings({"checkstyle:TrailingComment"})
public final class UringAsyncFile extends AsyncFile {

    public UringAsyncFile(String path,
                          Eventloop eventloop,
                          StorageScheduler scheduler,
                          StorageDevice device) {
        super(path, eventloop, scheduler, device);
    }


    private static IntPromise failOnOverload(IntPromise promise) {

//        promise.completeWithIOException(
//                "Overload. Max concurrent operations " + maxConcurrent + " dev: [" + dev.path() + "]", null);

        promise.completeWithIOException("No more IO available ", null);
        return promise;
    }


    @Override
    public IntPromise delete() {
        throw new RuntimeException("Not yet implemented");
    }


    @Override
    public long size() {
        return Linux.filesize(fd());
    }
}
