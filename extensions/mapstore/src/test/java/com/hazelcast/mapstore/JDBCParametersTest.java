package com.hazelcast.mapstore;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JDBCParametersTest {

    @Test
    public void testShiftParametersForUpdateForPosition0() {

        JDBCParameters jDBCParameters = new JDBCParameters();
        jDBCParameters.setParams(new Object[] {1,2,3});

        jDBCParameters.shiftParametersForUpdate();

        Object[] params = jDBCParameters.getParams();

        assertThat(params).isEqualTo(new Object[] {2,3,1});
    }

    @Test
    public void testShiftParametersForUpdateForPosition1() {

        JDBCParameters jDBCParameters = new JDBCParameters();
        jDBCParameters.setParams(new Object[] {1,2,3});
        jDBCParameters.setIdPos(1);

        jDBCParameters.shiftParametersForUpdate();

        Object[] params = jDBCParameters.getParams();

        assertThat(params).isEqualTo(new Object[] {1,3,2});
    }
}
