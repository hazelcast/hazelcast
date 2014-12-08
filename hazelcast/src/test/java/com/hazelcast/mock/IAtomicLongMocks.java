package com.hazelcast.mock;

import static com.hazelcast.mock.MockUtil.delegateTo;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicLong;

import com.hazelcast.core.IAtomicLong;

public class IAtomicLongMocks {

    /** Creates mocks IAtomicLong which wraps an AtomicLong **/
    public static IAtomicLong mockIAtomicLong() {
        final AtomicLong atomicLong = new AtomicLong(); // keeps actual value
        IAtomicLong iAtomicLong = mock(IAtomicLong.class);

        when( iAtomicLong.getAndIncrement() ).then( delegateTo(atomicLong) );
        when( iAtomicLong.compareAndSet(anyLong(), anyLong()) ).then( delegateTo(atomicLong) );

        return iAtomicLong;
    }
}
