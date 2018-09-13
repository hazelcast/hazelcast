package com.hazelcast.internal.probing;

import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.spi.annotation.PrivateApi;

@Beta
@PrivateApi
public interface ProbeRenderer {

    /**
     * Appends the given key value pair to the output in the format specific to the
     * renderer.
     * 
     * @param key not null, only guaranteed to be stable throughout the call
     * @param value -1 for unknown, >= 0 otherwise (double encoded as 10000 x double
     *        value)
     */
    void render(CharSequence key, long value);

}
