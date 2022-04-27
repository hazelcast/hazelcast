package com.hazelcast.spi.impl.engine.nio;

import com.hazelcast.internal.networking.nio.SelectorOptimizer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.engine.Channel;
import com.hazelcast.spi.impl.engine.Eventloop;
import com.hazelcast.spi.impl.engine.Scheduler;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public final class NioEventloop extends Eventloop {
    final Selector selector;

    public NioEventloop(int idx, String name, ILogger logger, Scheduler scheduler, boolean spin) {
        super(idx, name, logger, scheduler, spin);
        this.selector = SelectorOptimizer.newSelector(logger);
    }

    @Override
    public void wakeup() {
        if (spin || Thread.currentThread() == this) {
            return;
        }

        if (wakeupNeeded.get() && wakeupNeeded.compareAndSet(true, false)) {
            selector.wakeup();
        }
    }

    @Override
    protected void eventLoop() throws Exception {
        while (running) {
            runTasks();

            boolean moreWork = scheduler.tick();

            flushDirtyChannels();

            int keyCount;
            if (spin || moreWork) {
                keyCount = selector.selectNow();
            } else {
                wakeupNeeded.set(true);
                if (publicRunQueue.isEmpty()) {
                    keyCount = selector.select();
                } else {
                    keyCount = selector.selectNow();
                }
                wakeupNeeded.set(false);
            }

            if (keyCount > 0) {
                Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                while (it.hasNext()) {
                    SelectionKey key = it.next();
                    it.remove();

                    NioSelectedKeyListener listener = (NioSelectedKeyListener) key.attachment();
                    try {
                        listener.handle(key);
                    } catch (IOException e) {
                        listener.handleException(e);
                    }
                }
            }
        }
    }

    public void accept(NioServerChannel serverChannel) throws IOException {
        serverChannel.configure(this);
        schedule(serverChannel::accept);
    }

    @Override
    public CompletableFuture<Channel> connect(Channel c, SocketAddress address) {
        NioChannel channel = (NioChannel) c;

        CompletableFuture<Channel> future = new CompletableFuture<>();
        try {
            schedule(() -> channel.connect(future, address, NioEventloop.this));
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }
}
