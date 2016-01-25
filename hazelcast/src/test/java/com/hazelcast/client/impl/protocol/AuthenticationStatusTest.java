package com.hazelcast.client.impl.protocol;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AuthenticationStatusTest {

    @Test
    public void testGetId() throws Exception {
        assertEquals(0, AuthenticationStatus.AUTHENTICATED.getId());
        assertEquals(1, AuthenticationStatus.CREDENTIALS_FAILED.getId());
        assertEquals(2, AuthenticationStatus.SERIALIZATION_VERSION_MISMATCH.getId());
    }

    @Test
    public void testGetById() throws Exception {
        AuthenticationStatus status = AuthenticationStatus.getById(AuthenticationStatus.AUTHENTICATED.getId());
        assertEquals(AuthenticationStatus.AUTHENTICATED, status);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetById_invalidId() throws Exception {
        AuthenticationStatus.getById(-1);
    }
}
