package com.hazelcast.crdt.pncounter;

import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

/**
 * Base test for testing behaviour of PN counter behaviour when there are no data members in the cluster
 */
public abstract class BasePNCounterNoDataMemberTest extends HazelcastTestSupport {

    @Test(expected = NoDataMemberInClusterException.class)
    public void noDataMemberExceptionIsThrown() {
        final PNCounter driver = getCounter();
        mutate(driver);
    }

    protected abstract void mutate(PNCounter driver);

    protected abstract PNCounter getCounter();
}
