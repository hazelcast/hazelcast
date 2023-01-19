package com.hazelcast.mapstore;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JDBCParametersTest {

    @Test
    public void testShiftParametersForUpdate() {

        JDBCParameters jDBCParameters = new JDBCParameters();
        jDBCParameters.setParams(new Object[] {1,2,3});

        jDBCParameters.shiftParametersForUpdate();

        Object[] params = jDBCParameters.getParams();

        assertThat(params).isEqualTo(new Object[] {2,3,1});
    }
}
