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

import com.hazelcast.internal.tpcengine.file.AsyncFileMetrics;
import com.hazelcast.internal.tpcengine.file.BlockDevice;
import com.hazelcast.internal.tpcengine.file.BlockRequest;
import com.hazelcast.internal.tpcengine.file.BlockRequestScheduler;
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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_CREAT;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_DIRECT;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_RDONLY;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_RDWR;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_SYNC;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_TRUNC;
import static com.hazelcast.internal.tpcengine.file.AsyncFile.O_WRONLY;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_CLOSE;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_FALLOCATE;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_FDATASYNC;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_FSYNC;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_NOP;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_OPEN;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_READ;
import static com.hazelcast.internal.tpcengine.file.BlockRequest.BLK_REQ_OP_WRITE;

public class NioBlockRequestScheduler implements BlockRequestScheduler {
    private static final Executor EXECUTOR = Executors.newCachedThreadPool((Runnable r) -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
    });
    final MpscArrayQueue<NioBlockRequest> cq;
    private final NioReactor reactor;
    private final int concurrentLimit;
    private final CircularQueue<NioBlockRequest> waitQueue;
    private int concurrent;
    private final SlabAllocator<NioBlockRequest> requestAllocator;
    private final CompletionHandler<Integer, NioBlockRequest> rwCompletionHandler = new CompletionHandler<>() {
        @Override
        public void completed(Integer result, NioBlockRequest attachment) {
            attachment.result = result;
            cq.add(attachment);
            reactor.wakeup();
        }

        @Override
        public void failed(Throwable exc, NioBlockRequest attachment) {
            attachment.exc = exc;
            cq.add(attachment);
            reactor.wakeup();
        }
    };

    public NioBlockRequestScheduler(BlockDevice dev, NioEventloop nioEventloop) {
        concurrentLimit = dev.concurrentLimit();
        // is npe possible down the line?
        reactor = (NioReactor) nioEventloop.getReactor();
        waitQueue = new CircularQueue<>(dev.maxWaiting() - concurrentLimit);
        requestAllocator = new SlabAllocator<>(dev.maxWaiting(), NioBlockRequest::new);
        cq = new MpscArrayQueue<>(dev.maxWaiting());
    }

    @Override
    public BlockRequest reserve() {
        return requestAllocator.allocate();
    }

    @Override
    public void submit(BlockRequest req) {
        NioBlockRequest nioBlockRequest = (NioBlockRequest) req;
        if (concurrent < concurrentLimit) {
            submit0(nioBlockRequest);
            concurrent++;
        } else if (!waitQueue.offer(nioBlockRequest)) {
            throw new IllegalStateException("Too many concurrent IOs");
        }
    }

    private void submit0(NioBlockRequest req) {
        switch (req.opcode) {
            case BLK_REQ_OP_NOP:
                EXECUTOR.execute(() -> {
                    req.result = 0;
                    cq.add(req);
                    reactor.wakeup();
                });
                break;
            case BLK_REQ_OP_READ:
                req.getFile().channel.read(req.buffer.byteBuffer(), req.offset, req, rwCompletionHandler);
                break;
            case BLK_REQ_OP_WRITE:
                req.getFile().channel.write(req.buffer.byteBuffer(), req.offset, req, rwCompletionHandler);
                break;
            case BLK_REQ_OP_FSYNC:
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
            case BLK_REQ_OP_FDATASYNC:
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
            case BLK_REQ_OP_OPEN:
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
            case BLK_REQ_OP_CLOSE:
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
            case BLK_REQ_OP_FALLOCATE:
                throw new RuntimeException("Not implemented yet.");
            default:
                throw new IllegalStateException("Unknown request type: " + req.opcode);
        }
    }

    private OpenOption[] flagsToOpenOptions(int flags) {
        int p = (flags & 3);
        int supportedOpsExceptRW = (O_CREAT | O_TRUNC | O_DIRECT | O_SYNC);
        int len = Integer.bitCount(flags & supportedOpsExceptRW) + p == O_RDWR ? 2 : 1;
        // can be pooled
        OpenOption[] opts = new OpenOption[len];
        int i = 0;

        if (p == O_RDONLY) {
            opts[i++] = StandardOpenOption.READ;
        } else if (p == O_WRONLY) {
            opts[i++] = StandardOpenOption.WRITE;
        } else if (p == O_RDWR) {
            opts[i++] = StandardOpenOption.READ;
            opts[i++] = StandardOpenOption.WRITE;
        }

        if ((flags & O_CREAT) != 0) {
            opts[i++] = StandardOpenOption.CREATE;
        }

        if ((flags & O_TRUNC) != 0) {
            opts[i++] = StandardOpenOption.TRUNCATE_EXISTING;
        }

        if ((flags & O_DIRECT) != 0) {
            opts[i++] = ExtendedOpenOption.DIRECT;
        }

        if ((flags & O_SYNC) != 0) {
            opts[i++] = StandardOpenOption.SYNC;
        }

        assert i == len;

        return opts;
    }

    void complete(NioBlockRequest nioBlockRequest) {
        if (nioBlockRequest.exc != null) {
            nioBlockRequest.promise.completeExceptionally(nioBlockRequest.exc);
        } else {
            if (nioBlockRequest.opcode == BLK_REQ_OP_OPEN) {
                nioBlockRequest.getFile().channel = nioBlockRequest.channel;
            }
            nioBlockRequest.promise.complete(nioBlockRequest.result);
            handleMetrics(nioBlockRequest);
        }

        nioBlockRequest.rwFlags = 0;
        nioBlockRequest.flags = 0;
        nioBlockRequest.opcode = 0;
        nioBlockRequest.length = 0;
        nioBlockRequest.offset = 0;
        nioBlockRequest.permissions = 0;
        nioBlockRequest.buffer = null;
        nioBlockRequest.file = null;
        nioBlockRequest.promise = null;
        nioBlockRequest.result = 0;
        nioBlockRequest.exc = null;
        requestAllocator.free(nioBlockRequest);
        concurrent--;

        while (waitQueue.hasRemaining() && concurrent < concurrentLimit) {
            submit(waitQueue.poll());
        }
    }

    private void handleMetrics(NioBlockRequest nioBlockRequest) {
        AsyncFileMetrics metrics = nioBlockRequest.file.metrics();
        int res = nioBlockRequest.result;
        switch (nioBlockRequest.opcode) {
            case BLK_REQ_OP_NOP:
                metrics.incNops();
                break;
            case BLK_REQ_OP_READ:
                metrics.incReads();
                metrics.incBytesRead(res);
                break;
            case BLK_REQ_OP_WRITE:
                metrics.incWrites();
                metrics.incBytesWritten(res);
                break;
            case BLK_REQ_OP_FSYNC:
                metrics.incFsyncs();
                break;
            case BLK_REQ_OP_FDATASYNC:
                metrics.incFdatasyncs();
                break;
            case BLK_REQ_OP_OPEN:
            case BLK_REQ_OP_CLOSE:
            case BLK_REQ_OP_FALLOCATE:
                break;
            default:
                throw new IllegalStateException("Unknown opcode: " + nioBlockRequest.opcode);
        }
    }

    public static final class NioBlockRequest extends BlockRequest {
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
