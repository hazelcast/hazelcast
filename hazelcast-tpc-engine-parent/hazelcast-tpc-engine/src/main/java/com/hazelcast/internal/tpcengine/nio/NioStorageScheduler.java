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

import com.hazelcast.internal.tpcengine.file.AsyncFile;
import com.hazelcast.internal.tpcengine.file.StorageDevice;
import com.hazelcast.internal.tpcengine.file.StorageRequest;
import com.hazelcast.internal.tpcengine.file.StorageScheduler;
import com.hazelcast.internal.tpcengine.util.CircularQueue;
import com.hazelcast.internal.tpcengine.util.SlabAllocator;
import com.sun.nio.file.ExtendedOpenOption;
import org.jctools.queues.MpscArrayQueue;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_CREAT;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_DIRECT;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_RDONLY;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_RDWR;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_SYNC;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_TRUNC;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_WRONLY;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_CLOSE;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_FALLOCATE;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_FDATASYNC;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_FSYNC;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_NOP;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_OPEN;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_READ;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_WRITE;

@SuppressWarnings({
        "checkstyle:CyclomaticComplexity",
        "checkstyle:VisibilityModifier",
        "checkstyle:MethodLength"})
public class NioStorageScheduler implements StorageScheduler {

    private static final Executor EXECUTOR = Executors.newCachedThreadPool((Runnable r) -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
    });

    final MpscArrayQueue<NioStorageRequest> cq;
    private final NioReactor reactor;
    private final int concurrentLimit;
    private final CircularQueue<NioStorageRequest> waitQueue;
    private int concurrent;
    private final SlabAllocator<NioStorageRequest> requestAllocator;
    private final CompletionHandler<Integer, NioStorageRequest> rwCompletionHandler = new CompletionHandler<>() {
        @Override
        public void completed(Integer result, NioStorageRequest attachment) {
            attachment.result = result;
            cq.add(attachment);
            reactor.wakeup();
        }

        @Override
        public void failed(Throwable exc, NioStorageRequest attachment) {
            attachment.exc = exc;
            cq.add(attachment);
            reactor.wakeup();
        }
    };

    public NioStorageScheduler(StorageDevice dev, NioEventloop nioEventloop) {
        concurrentLimit = dev.concurrentLimit();
        // is npe possible down the line?
        reactor = (NioReactor) nioEventloop.reactor();
        waitQueue = new CircularQueue<>(dev.maxWaiting() - concurrentLimit);
        requestAllocator = new SlabAllocator<>(dev.maxWaiting(), NioStorageRequest::new);
        cq = new MpscArrayQueue<>(dev.maxWaiting());
    }

    @Override
    public StorageRequest allocate() {
        return requestAllocator.allocate();
    }

    @Override
    public void schedule(StorageRequest req) {
        NioStorageRequest nioBlockRequest = (NioStorageRequest) req;
        if (concurrent < concurrentLimit) {
            submit0(nioBlockRequest);
            concurrent++;
        } else if (!waitQueue.offer(nioBlockRequest)) {
            throw new IllegalStateException("Too many concurrent IOs");
        }
    }

    private void submit0(NioStorageRequest req) {
        switch (req.opcode) {
            case STR_REQ_OP_NOP:
                EXECUTOR.execute(() -> {
                    req.result = 0;
                    cq.add(req);
                    reactor.wakeup();
                });
                break;
            case STR_REQ_OP_READ:
                req.getFile().channel.read(req.buffer.byteBuffer(), req.offset, req, rwCompletionHandler);
                break;
            case STR_REQ_OP_WRITE:
                req.getFile().channel.write(req.buffer.byteBuffer(), req.offset, req, rwCompletionHandler);
                break;
            case STR_REQ_OP_FSYNC:
                EXECUTOR.execute(() -> {
                    try {
                        req.getFile().channel.force(true);
                        req.result = 0;
                    } catch (IOException e) {
                        req.exc = e;
                    }

                    cq.add(req);
                    reactor.wakeup();
                });
                break;
            case STR_REQ_OP_FDATASYNC:
                EXECUTOR.execute(() -> {
                    try {
                        req.getFile().channel.force(false);
                        req.result = 0;
                    } catch (IOException e) {
                        req.exc = e;
                    }

                    cq.add(req);
                    reactor.wakeup();
                });
                break;
            case STR_REQ_OP_OPEN:
                EXECUTOR.execute(() -> {
                    try {
                        req.channel = AsynchronousFileChannel.open(
                                Path.of(req.file.path()),
                                flagsToOpenOptions(req.flags));
                        req.result = 0;
                    } catch (IOException e) {
                        req.exc = e;
                    }

                    cq.add(req);
                    reactor.wakeup();
                });
                break;
            case STR_REQ_OP_CLOSE:
                EXECUTOR.execute(() -> {
                    try {
                        req.getFile().channel.close();
                        req.result = 0;
                    } catch (IOException e) {
                        req.exc = e;
                    }

                    cq.add(req);
                    reactor.wakeup();
                });
                break;
            case STR_REQ_OP_FALLOCATE:
                throw new RuntimeException("Not implemented yet.");
            default:
                throw new IllegalStateException("Unknown request type: " + req.opcode);
        }
    }

    // todo: The opts list and array can be pooled to reduce litter.
    private OpenOption[] flagsToOpenOptions(int flags) {
        int p = (flags & 3);
        int supportedOpsExceptRW = (O_CREAT | O_TRUNC | O_DIRECT | O_SYNC);
        int len = Integer.bitCount(flags & supportedOpsExceptRW) + p == O_RDWR ? 2 : 1;
        // can be pooled
        List<OpenOption> opts = new ArrayList<OpenOption>();

        if (p == O_RDONLY) {
            opts.add(StandardOpenOption.READ);
        } else if (p == O_WRONLY) {
            opts.add(StandardOpenOption.WRITE);
        } else if (p == O_RDWR) {
            opts.add(StandardOpenOption.READ);
            opts.add(StandardOpenOption.WRITE);
        }

        if ((flags & O_CREAT) != 0) {
            opts.add(StandardOpenOption.CREATE);
        }

        if ((flags & O_TRUNC) != 0) {
            opts.add(StandardOpenOption.TRUNCATE_EXISTING);
        }

        if ((flags & O_DIRECT) != 0) {
            opts.add(ExtendedOpenOption.DIRECT);
        }

        if ((flags & O_SYNC) != 0) {
            opts.add(StandardOpenOption.SYNC);
        }

        return opts.toArray(new OpenOption[0]);
    }

    void complete(NioStorageRequest req) {
        if (req.exc != null) {
            req.promise.completeExceptionally(req.exc);
        } else {
            if (req.opcode == STR_REQ_OP_OPEN) {
                req.getFile().channel = req.channel;
            }
            req.promise.complete(req.result);
            handleMetrics(req);
        }

        req.rwFlags = 0;
        req.flags = 0;
        req.opcode = 0;
        req.length = 0;
        req.offset = 0;
        req.permissions = 0;
        req.buffer = null;
        req.file = null;
        req.promise = null;
        req.result = 0;
        req.exc = null;
        requestAllocator.free(req);
        concurrent--;

        while (waitQueue.hasRemaining() && concurrent < concurrentLimit) {
            schedule(waitQueue.poll());
        }
    }

    private void handleMetrics(NioStorageRequest nioBlockRequest) {
        AsyncFile.Metrics metrics = nioBlockRequest.file.metrics();
        int res = nioBlockRequest.result;
        switch (nioBlockRequest.opcode) {
            case STR_REQ_OP_NOP:
                metrics.incNops();
                break;
            case STR_REQ_OP_READ:
                metrics.incReads();
                metrics.incBytesRead(res);
                break;
            case STR_REQ_OP_WRITE:
                metrics.incWrites();
                metrics.incBytesWritten(res);
                break;
            case STR_REQ_OP_FSYNC:
                metrics.incFsyncs();
                break;
            case STR_REQ_OP_FDATASYNC:
                metrics.incFdatasyncs();
                break;
            case STR_REQ_OP_OPEN:
            case STR_REQ_OP_CLOSE:
            case STR_REQ_OP_FALLOCATE:
                break;
            default:
                throw new IllegalStateException("Unknown opcode: " + nioBlockRequest.opcode);
        }
    }

    public static final class NioStorageRequest extends StorageRequest {
        // only modify these 3 fields in io threads
        // don't complete promise etc.
        public int result;
        public Throwable exc;
        public AsynchronousFileChannel channel;

        public NioAsyncFile getFile() {
            return (NioAsyncFile) file;
        }
    }
}
