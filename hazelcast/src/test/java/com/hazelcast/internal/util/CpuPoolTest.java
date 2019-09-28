package com.hazelcast.internal.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CpuPoolTest {

    @Test
    public void affinityCpuStringToList_whenEmpty(){
        List<Integer> cpus = CpuPool.affinityCpuStringToList("");
        assertTrue(cpus.isEmpty());
    }

    @Test
    public void affinityCpuStringToList_whenSingleValue(){
        List<Integer> cpus = CpuPool.affinityCpuStringToList("2");
        assertEquals(Arrays.asList(2), cpus);
    }

    @Test
    public void affinityCpuStringToList_whenCommaSeperatedValues(){
        List<Integer> cpus = CpuPool.affinityCpuStringToList("2,10,3,4,11");
        assertEquals(Arrays.asList(2, 10, 3, 4, 11), cpus);
    }

    @Test
    public void affinityCpuStringToList_whenCommaSeperatedValuesAndDuplicated(){
        List<Integer> cpus = CpuPool.affinityCpuStringToList("2,2");
        assertEquals(Arrays.asList(2), cpus);
    }

    @Test
    public void affinityCpuStringToList_whenRange(){
        List<Integer> cpus = CpuPool.affinityCpuStringToList("2-6");
        assertEquals(Arrays.asList(2,3,4,5,6), cpus);
    }

    @Test
    public void affinityCpuStringToList_whenRangeAndComma(){
        List<Integer> cpus = CpuPool.affinityCpuStringToList("1,3-6,10,15-20");
        assertEquals(Arrays.asList(1,3,4,5,6,10,15,16,17,18,19,20), cpus);
    }
}
