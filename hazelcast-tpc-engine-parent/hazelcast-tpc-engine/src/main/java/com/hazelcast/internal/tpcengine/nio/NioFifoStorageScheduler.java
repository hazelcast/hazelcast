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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

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
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;
import static java.lang.Math.min;

/**
 * Nio {@link StorageScheduler} implementation.
 * <p>
 * It is a FIFO based scheduler where all requests end up in a single requestQueue.
 * Requests are picked from this request queue until the queue is empty or until
 * the maximum ioDepth has been reached.
 * <p/>
 * The actual storage requests are processed by an executor. Where each thread performs
 * one blocking operation. So the size of the threadpool determines the number of IOPS.
 */
@SuppressWarnings({
        "checkstyle:CyclomaticComplexity",
        "checkstyle:VisibilityModifier",
        "checkstyle:MethodLength"})
public class NioFifoStorageScheduler implements StorageScheduler {

    private final NioReactor reactor;
    // The queue within the executor is the submission queue.
    private final ExecutorService executor;
    private final SlabAllocator<NioStorageRequest> requestAllocator;
    private final CompletionHandler<Integer, NioStorageRequest> handler = new StorageRequestCompletionHandler();
    // This is where completed requests end up
    private final MpscArrayQueue<NioStorageRequest> completionQueue;
    // This is where the scheduled request first end up
    private final CircularQueue<NioStorageRequest> stagingQueue;
    private final int submitLimit;
    // the number of request that are currently submitted; so the
    // number of requests offered to the executor
    private int submitCount;

    /**
     * Creates a NioFifoStorageScheduler.
     *
     * @param reactor      the NioReactor this scheduler belongs to.
     * @param executor     the executor that processes the storage requests.
     * @param submitLimit  the limit on the number of storage requests submitted
     *                     for processing; so are actually being offered to the
     *                     executor for processing).
     * @param pendingLimit the limit on the number of storage request pending;
     *                     so are either staged or submitted.
     */
    public NioFifoStorageScheduler(NioReactor reactor,
                                   ExecutorService executor,
                                   int submitLimit,
                                   int pendingLimit) {
        this.submitLimit = checkPositive(submitLimit, "submitLimit");
        if (pendingLimit < submitLimit) {
            throw new IllegalArgumentException();
        }
        this.reactor = checkNotNull(reactor, "reactor");
        this.executor = checkNotNull(executor, "executor");
        this.requestAllocator = new SlabAllocator<>(pendingLimit, NioStorageRequest::new);
        this.stagingQueue = new CircularQueue<>(pendingLimit);
        // Needs to be thread safe because the completion is send from
        // a thread from the executor.
        this.completionQueue = new MpscArrayQueue<>(submitLimit);
    }

    @Override
    public StorageRequest allocate() {
        return requestAllocator.allocate();
    }

    @Override
    public void schedule(StorageRequest r) {
        if (!stagingQueue.offer((NioStorageRequest) r)) {
            // This should not happen because the allocate protect against overload.
            throw new IllegalStateException("Too many concurrent StorageRequests");
        }
    }

    @Override
    public boolean tick() {
        int completed = processCompleted();
        submit();
        return completed > 0;
    }

    // todo: better name
    public boolean hasPending() {
        return !completionQueue.isEmpty();
    }

    /**
     * Takes as many requests from the staging queue as allowed and submits them
     * for actual processing.
     */
    private void submit() {
        int c = min(submitLimit - submitCount, stagingQueue.size());
        for (int k = 0; k < c; k++) {
            NioStorageRequest req = stagingQueue.poll();
            submit(req);
        }
    }

    private void submit(NioStorageRequest req) {
        submitCount++;
        switch (req.opcode) {
            case STR_REQ_OP_NOP:
                executor.execute(() -> {
                    req.result = 0;
                    completionQueue.add(req);
                    reactor.wakeup();
                });
                break;
            case STR_REQ_OP_READ:
                req.getFile().channel.read(req.buffer.byteBuffer(), req.offset, req, handler);
                break;
            case STR_REQ_OP_WRITE:
                req.getFile().channel.write(req.buffer.byteBuffer(), req.offset, req, handler);
                break;
            case STR_REQ_OP_FSYNC:
                executor.execute(() -> {
                    try {
                        req.getFile().channel.force(true);
                        req.result = 0;
                    } catch (IOException e) {
                        req.exc = e;
                    }

                    completionQueue.add(req);
                    reactor.wakeup();
                });
                break;
            case STR_REQ_OP_FDATASYNC:
                executor.execute(() -> {
                    try {
                        req.getFile().channel.force(false);
                        req.result = 0;
                    } catch (IOException e) {
                        req.exc = e;
                    }

                    completionQueue.add(req);
                    reactor.wakeup();
                });
                break;
            case STR_REQ_OP_OPEN:
                executor.execute(() -> {
                    try {
                        Path path = Path.of(req.file.path());
                        Set<OpenOption> options = flagsToOpenOptions(req.flags);
                        req.channel = AsynchronousFileChannel.open(path, options, executor);
                        req.result = 0;
                    } catch (IOException e) {
                        req.exc = e;
                    }

                    completionQueue.add(req);
                    reactor.wakeup();
                });
                break;
            case STR_REQ_OP_CLOSE:
                executor.execute(() -> {
                    try {
                        req.getFile().channel.close();
                        req.result = 0;
                    } catch (IOException e) {
                        req.exc = e;
                    }

                    completionQueue.add(req);
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
    private Set<OpenOption> flagsToOpenOptions(int flags) {
        int p = (flags & 3);
        int supportedOpsExceptRW = (O_CREAT | O_TRUNC | O_DIRECT | O_SYNC);
        int len = Integer.bitCount(flags & supportedOpsExceptRW) + p == O_RDWR ? 2 : 1;
        // can be pooled
        Set<OpenOption> opts = new HashSet<>();

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

        return opts;
    }

    /**
     * Processes all the completed StorageRequests.
     *
     * @return
     */
    private int processCompleted() {
        int completed = 0;
        for (; ; ) {
            NioStorageRequest req = completionQueue.poll();
            if (req == null) {
                break;
            }
            complete(req);
            completed++;
        }
        return completed;
    }

    void complete(NioStorageRequest req) {
        if (req.exc != null) {
            req.callback.accept(-1, req.exc);
        } else {
            if (req.opcode == STR_REQ_OP_OPEN) {
                req.getFile().channel = req.channel;
            }
            req.callback.accept(req.result, null);
            updateMetrics(req);
        }

        req.rwFlags = 0;
        req.flags = 0;
        req.opcode = 0;
        req.length = 0;
        req.offset = 0;
        req.permissions = 0;
        req.buffer = null;
        req.file = null;
        req.callback = null;
        req.result = 0;
        req.exc = null;
        requestAllocator.free(req);
        submitCount--;
    }

    private void updateMetrics(NioStorageRequest req) {
        AsyncFile.Metrics metrics = req.file.metrics();
        int res = req.result;
        switch (req.opcode) {
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
                throw new IllegalStateException("Unknown opcode: " + req.opcode);
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

    private class StorageRequestCompletionHandler implements CompletionHandler<Integer, NioStorageRequest> {
        @Override
        public void completed(Integer result, NioStorageRequest req) {
            req.result = result;
            completionQueue.add(req);
            reactor.wakeup();
        }

        @Override
        public void failed(Throwable exc, NioStorageRequest req) {
            System.out.println("Failure!!!");
            req.exc = exc;
            completionQueue.add(req);
            reactor.wakeup();
        }
    }
}
