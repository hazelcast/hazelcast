package com.hazelcast.nio.tcp;

/**
 * Default {@link com.hazelcast.nio.tcp.IOEventLoopFactory} implementation.
 */
public final class DefaultIOEventLoopFactory implements IOEventLoopFactory {

    private boolean spinningEnabled;

    /**
     * Sets if the event loop should rely on spinning or not.
     *
     * @param spinningEnabled if the event loop should rely on spinning.
     * @return the updated DefaultIOEventLoopFactory.
     */
    public DefaultIOEventLoopFactory setSpinningEnabled(boolean spinningEnabled) {
        this.spinningEnabled = spinningEnabled;
        return this;
    }

    @Override
    public IOEventLoop create(IOReactor reactor) {
        if (spinningEnabled) {
            return new SpinningIOEventLoop(reactor);
        } else {
            return new BlockingIOEventLoop(reactor);
        }
    }
}
