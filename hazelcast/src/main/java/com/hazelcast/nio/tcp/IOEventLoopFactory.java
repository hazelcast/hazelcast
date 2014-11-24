package com.hazelcast.nio.tcp;

/**
 * A factory for creating {@link com.hazelcast.nio.tcp.IOEventLoop} instances.
 *
 * The factory gives us the ability to inject different IOEventLoops into the IOReactor. Currently there
 * is one implementation, but there are others possible:
 * - busy spin event loop that relies on selector.selectNow
 * - event loop with the Netty garbage collection fix so that the hashset returned by the selector doesn't create
 * too much garbage.
 */
public interface IOEventLoopFactory {

    /**
     * Creates the IOEventLoop.
     *
     * @param reactor the reactor that is going to use this event loop.
     * @return the created IOEventLoop.
     */
    IOEventLoop create(IOReactor reactor);
}
