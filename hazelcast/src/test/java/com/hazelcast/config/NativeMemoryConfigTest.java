package com.hazelcast.config;

import com.hazelcast.memory.Capacity;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NativeMemoryConfigTest {

    @Test
    public void equals_and_hashcode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(NativeMemoryConfig.class)
                      .suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS)
                      .verify();
    }

    @Test
    @SuppressWarnings("deprecation")
    public void size_and_capacity_are_the_same() {
        NativeMemoryConfig confSize = new NativeMemoryConfig().setSize(MemorySize.parse("1337", MEGABYTES));
        NativeMemoryConfig confCapacity = new NativeMemoryConfig().setCapacity(Capacity.parse("1337", MEGABYTES));

        assertThat(confSize.getSize()).isEqualTo(confCapacity.getSize());
        assertThat(confSize.getCapacity()).isEqualTo(confCapacity.getCapacity());
        assertThat(confSize.getCapacity()).isEqualTo(confCapacity.getSize());
    }

}