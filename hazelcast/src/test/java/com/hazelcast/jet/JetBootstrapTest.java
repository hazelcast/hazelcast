package com.hazelcast.jet;

import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelJVMTest.class)
public class JetBootstrapTest {

    @Test
    public void bootstrappedInstance() {
        JetInstance instance = Jet.bootstrappedInstance();
        instance.shutdown();
    }
}