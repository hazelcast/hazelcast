package com.hazelcast.crdt.pncounter;

import com.hazelcast.core.ConsistencyLostException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

/**
 * Base test for testing behaviour of {@link ConsistencyLostException} in the case of CRDTs.
 */
public abstract class BasePNCounterConsistencyLostTest extends HazelcastTestSupport {

    @Test(expected = ConsistencyLostException.class)
    public void consistencyLostExceptionIsThrownWhenTargetReplicaDisappears() {
        final PNCounter driver = getCounter();
        mutate(driver);
        assertState(driver);

        final Address currentTarget = getCurrentTargetReplicaAddress(driver);

        terminateMember(currentTarget);

        mutate(driver);
    }

    @Test
    public void driverCanContinueSessionByCallingReset() {
        final PNCounter driver = getCounter();
        mutate(driver);
        assertState(driver);
        final Address currentTarget = getCurrentTargetReplicaAddress(driver);
        terminateMember(currentTarget);

        driver.reset();
        mutate(driver);
    }

    private void terminateMember(Address memberAddress) {
        for (HazelcastInstance member : getMembers()) {
            if (getNode(member).getThisAddress().equals(memberAddress)) {
                TestUtil.terminateInstance(member);
            }
        }
    }

    protected abstract Address getCurrentTargetReplicaAddress(PNCounter driver);

    protected abstract void assertState(PNCounter driver);

    protected abstract void mutate(PNCounter driver);

    protected abstract PNCounter getCounter();

    protected abstract HazelcastInstance[] getMembers();
}
