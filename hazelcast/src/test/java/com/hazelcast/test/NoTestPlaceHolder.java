package com.hazelcast.test;

import com.hazelcast.test.annotation.NoTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * @mdogan 5/29/13
 */

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(NoTest.class)
public class NoTestPlaceHolder {

    @Test
    public void test() {
        System.out.println("Run tests using one of profiles; 'parallel-test', 'serial-test' or 'all-tests' !!!");
    }
}
