/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet2.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public final class Util {

    private static final int KILOBYTE = 1024;

    private Util() {
    }

    public static <T> T uncheckedGet(Future<T> f) {
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw rethrow(e);
        }
    }

    public static Throwable peel(Throwable e) {
        if (e instanceof CompletionException) {
            return peel(e.getCause());
        }
        return e;
    }

    public static CompletableFuture<Object> allOf(Collection<ICompletableFuture> futures) {
        final CompletableFuture<Object> compositeFuture = new CompletableFuture<>();
        final AtomicInteger completionLatch = new AtomicInteger(futures.size());
        for (ICompletableFuture future : futures) {
            future.andThen(new ExecutionCallback() {
                @Override
                public void onResponse(Object response) {
                    if (completionLatch.decrementAndGet() == 0) {
                        compositeFuture.complete(true);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    compositeFuture.completeExceptionally(t);
                }
            });
        }
        return compositeFuture;
    }

    public static byte[] read(InputStream in, int count) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] b = new byte[count];
            out.write(b, 0, in.read(b));
            return out.toByteArray();
        }
    }

    public static byte[] read(InputStream in) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] b = new byte[KILOBYTE];
            for (int len; (len = in.read(b)) != -1; ) {
                out.write(b, 0, len);
            }
            return out.toByteArray();
        }
    }
}
