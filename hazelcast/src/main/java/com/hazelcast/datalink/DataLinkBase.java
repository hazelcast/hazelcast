package com.hazelcast.datalink;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.datalink.impl.ReferenceCounter;

import javax.annotation.Nonnull;

/**
 * Base class for {@link DataLink} implementations. Provides a {@link
 * ReferenceCounter}. When the ref count gets to 0, calls the {@link #destroy()}
 * method.
 */
public abstract class DataLinkBase implements DataLink {

    private final ReferenceCounter refCounter = new ReferenceCounter(this::destroy);
    private final DataLinkConfig config;

    protected DataLinkBase(@Nonnull DataLinkConfig config) {
        this.config = config;
    }

    @Override
    @Nonnull
    public final String getName() {
        return config.getName();
    }

    @Override
    public final void retain() {
        refCounter.retain();
    }

    @Override
    public final void release() {
        try {
            refCounter.release();
        } catch (Throwable e) {
            // Any DataLink user might be calling the release method. There can be 3rd party
            // errors here from closing the resources, we want to shield the user from those.
            // We don't have a logger here, for that we'd need to have NodeEngine reference, but
            // the constructor takes only the config, so we just print to the console.
            e.printStackTrace();
        }
    }

    @Nonnull
    @Override
    public final DataLinkConfig getConfig() {
        return config;
    }
}
