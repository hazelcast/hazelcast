package com.hazelcast.core;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DistributedObjectUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(DistributedObjectUtil.class);
    }

    @Test
    public void testGetName() {
        DistributedObject distributedObject = mock(DistributedObject.class);
        when(distributedObject.getName()).thenReturn("MockedDistributedObject");

        String name = DistributedObjectUtil.getName(distributedObject);

        assertEquals("MockedDistributedObject", name);
        verify(distributedObject).getName();
        verifyNoMoreInteractions(distributedObject);
    }

    @Test
    public void testGetName_withPrefixedDistributedObject() {
        PrefixedDistributedObject distributedObject = mock(PrefixedDistributedObject.class);
        when(distributedObject.getPrefixedName()).thenReturn("MockedPrefixedDistributedObject");

        String name = DistributedObjectUtil.getName(distributedObject);

        assertEquals("MockedPrefixedDistributedObject", name);
        verify(distributedObject).getPrefixedName();
        verifyNoMoreInteractions(distributedObject);
    }
}
