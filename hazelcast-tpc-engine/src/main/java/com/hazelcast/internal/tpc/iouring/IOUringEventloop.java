package com.hazelcast.internal.tpc.iouring;

import com.hazelcast.internal.tpc.AsyncFile;
import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.Scheduler;
import com.hazelcast.internal.tpc.iobuffer.IOBufferAllocator;
import com.hazelcast.internal.tpc.iobuffer.NonConcurrentIOBufferAllocator;
import com.hazelcast.internal.tpc.util.LongObjectHashMap;
import com.hazelcast.internal.tpc.util.NanoClock;
import com.hazelcast.internal.tpc.util.UnsafeLocator;
import org.jctools.queues.MpmcArrayQueue;
import sun.misc.Unsafe;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_READ;
import static com.hazelcast.internal.tpc.iouring.IOUring.IORING_OP_TIMEOUT;
import static com.hazelcast.internal.tpc.util.BitUtil.SIZEOF_LONG;
import static com.hazelcast.internal.tpc.util.CloseUtil.closeAllQuietly;
import static com.hazelcast.internal.tpc.util.CloseUtil.closeQuietly;
import static com.hazelcast.internal.tpc.util.OS.pageSize;

public class IOUringEventloop extends Eventloop {
    private final static Unsafe UNSAFE = UnsafeLocator.UNSAFE;

    private final IOUringReactor ioUringReactor;
    private final StorageDeviceRegistry storageScheduler;

    private final IOUring uring;

    final LongObjectHashMap<IOCompletionHandler> handlers = new LongObjectHashMap<>(4096);

    // this is not a very efficient allocator. It would be better to allocate a large chunk of
    // memory and then carve out smaller blocks. But for now it will do.
    private final IOBufferAllocator storeIOBufferAllocator = new NonConcurrentIOBufferAllocator(4096, true, pageSize());
    final SubmissionQueue sq;
    private final CompletionQueue cq;
    private final EventloopHandler eventLoopHandler;
    private final long userdata_eventRead;
    private final long userdata_timeout;
    private final long timeoutSpecAddr = UNSAFE.allocateMemory(Linux.SIZEOF_KERNEL_TIMESPEC);

    final EventFd eventfd = new EventFd();
    private final long eventFdReadBuf = UNSAFE.allocateMemory(SIZEOF_LONG);

    private long permanentHandlerIdGenerator = 0;
    private long temporaryHandlerIdGenerator = -1;

    public IOUringEventloop(IOUringReactor reactor, IOUringReactorBuilder builder) {
        super(reactor, builder);
        this.ioUringReactor = reactor;
        this.storageScheduler = reactor.storageScheduler;

        // The uring instance needs to be created on the eventloop thread.
        // This is required for some of the setup flags.
        this.uring = new IOUring(builder.entries, builder.setupFlags);
        if (builder.registerRing) {
            this.uring.registerRingFd();
        }
        this.sq = uring.getSubmissionQueue();
        this.cq = uring.getCompletionQueue();

        this.eventLoopHandler = new EventloopHandler();
        //todo: ugly, should not be null
        if (storageScheduler != null) {
            storageScheduler.init(this);
        }
        this.userdata_eventRead = nextPermanentHandlerId();
        this.userdata_timeout = nextPermanentHandlerId();
        handlers.put(userdata_eventRead, new EventFdCompletionHandler());
        handlers.put(userdata_timeout, new TimeoutCompletionHandler());
    }

    /**
     * Gets the next handler id for a permanent handler. A permanent handler stays registered after receiving
     * a completion event.
     *
     * @return the next handler id.
     */
    public long nextPermanentHandlerId() {
        return permanentHandlerIdGenerator++;
    }

    /**
     * Gets the next handler id for a temporary handler. A temporary handler is automatically removed after receiving
     * the completion event.
     *
     * @return the next handler id.
     */
    public long nextTemporaryHandlerId() {
        return temporaryHandlerIdGenerator--;
    }

    @Override
    public IOBufferAllocator fileIOBufferAllocator() {
        return storeIOBufferAllocator;
    }

    @Override
    public AsyncFile newAsyncFile(String path) {
        return new IOUringAsyncFile(path, ioUringReactor);
    }

    @Override
    protected void run() throws Exception {
        final NanoClock nanoClock = this.nanoClock;
        final EventloopHandler eventLoopHandler = this.eventLoopHandler;
        final AtomicBoolean wakeupNeeded = this.wakeupNeeded;
        final CompletionQueue cq = this.cq;
        final boolean spin = this.spin;
        final SubmissionQueue sq = this.sq;
        final MpmcArrayQueue externalTaskQueue = this.externalTaskQueue;
        final Scheduler scheduler = this.scheduler;

        sq_offerEventFdRead();

        boolean moreWork = false;
        do {
            if (cq.hasCompletions()) {
                // todo: do we want to control number of events being processed.
                cq.process(eventLoopHandler);
            } else {
                if (spin || moreWork) {
                    sq.submit();
                } else {
                    wakeupNeeded.set(true);
                    if (externalTaskQueue.isEmpty()) {
                        if (earliestDeadlineNanos != -1) {
                            long timeoutNanos = earliestDeadlineNanos - nanoClock.nanoTime();
                            if (timeoutNanos > 0) {
                                sq_offerTimeout(timeoutNanos);
                                sq.submitAndWait();
                            } else {
                                sq.submit();
                            }
                        } else {
                            sq.submitAndWait();
                        }
                    } else {
                        sq.submit();
                    }
                    wakeupNeeded.set(false);
                }
            }

            // what are the queues that are available for processing
            // 1: completion events
            // 2: concurrent task queue
            // 3: timed task queue
            // 4: local task queue
            // 5: scheduler task queue

            moreWork = runExternalTasks();
            moreWork |= scheduler.tick();
            moreWork |= runScheduledTasks();
            moreWork |= runLocalTasks();
        } while (!stop);
    }

    @Override
    protected void destroy() {
        closeQuietly(uring);
        closeQuietly(eventfd);
        closeAllQuietly(ioUringReactor.closeables);
        ioUringReactor.closeables.clear();

        if (timeoutSpecAddr != 0) {
            UNSAFE.freeMemory(timeoutSpecAddr);
        }

        if (eventFdReadBuf != 0) {
            UNSAFE.freeMemory(eventFdReadBuf);
        }
    }

    // todo: I'm questioning of this is not going to lead to problems. Can it happen that
    // multiple timeout requests are offered? So one timeout request is scheduled while another command is
    // already in the pipeline. Then the thread waits, and this earlier command completes while the later
    // timeout command is still scheduled. If another timeout is scheduled, then you have 2 timeouts in the
    // uring and both share the same timeoutSpecAddr.
    private void sq_offerTimeout(long timeoutNanos) {
        if (timeoutNanos <= 0) {
            UNSAFE.putLong(timeoutSpecAddr, 0);
            UNSAFE.putLong(timeoutSpecAddr + SIZEOF_LONG, 0);
        } else {
            long seconds = timeoutNanos / 1_000_000_000;
            UNSAFE.putLong(timeoutSpecAddr, seconds);
            UNSAFE.putLong(timeoutSpecAddr + SIZEOF_LONG, timeoutNanos - seconds * 1_000_000_000);
        }

        // todo: return value isn't checked
        sq.offer(IORING_OP_TIMEOUT,
                0,
                0,
                -1,
                timeoutSpecAddr,
                1,
                0,
                userdata_timeout);
    }

    private void sq_offerEventFdRead() {
        // todo: we are not checking return value.
        sq.offer(IORING_OP_READ,
                0,
                0,
                eventfd.fd(),
                eventFdReadBuf,
                SIZEOF_LONG,
                0,
                userdata_eventRead);
    }


    private class EventloopHandler implements IOCompletionHandler {
        final LongObjectHashMap<IOCompletionHandler> handlers = IOUringEventloop.this.handlers;

        @Override
        public void handle(int res, int flags, long userdata) {
            // Temporary handlers have a userdata smaller than 0 and need to be removed
            // on completion.
            // Permanent handlers have a userdata equal or larger than 0 and should not
            // be removed on completion.
            IOCompletionHandler h = userdata >= 0
                    ? handlers.get(userdata)
                    : handlers.remove(userdata);

            if (h == null) {
                logger.warning("no handler found for: " + userdata);
            } else {
                h.handle(res, flags, userdata);
            }
        }
    }

    private class EventFdCompletionHandler implements IOCompletionHandler {
        @Override
        public void handle(int res, int flags, long userdata) {
            sq_offerEventFdRead();
        }
    }

    private class TimeoutCompletionHandler implements IOCompletionHandler {
        @Override
        public void handle(int res, int flags, long userdata) {
        }
    }
}
