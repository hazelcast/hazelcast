package com.hazelcast.internal.probing;

public interface ProbeRenderer {

    void render(CharSequence key, long value);

}
