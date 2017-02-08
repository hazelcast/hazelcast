package com.hazelcast.version;

import com.hazelcast.nio.Version;
import org.junit.Test;

import static com.hazelcast.nio.Version.of;
import static com.hazelcast.version.VersionAssertion.assertThat;

public class VersionAssertionTest {

    @Test(expected = IllegalVersionException.class)
    public void lessThan_fail_equal() throws Exception {
        assertThat(of(8)).isLessThan(of(8));
    }

    @Test(expected = IllegalVersionException.class)
    public void lessThan_fail_less() throws Exception {
        assertThat(of(8)).isLessThan(of(7));
    }

    @Test
    public void lessThan() throws Exception {
        assertThat(of(8)).isLessThan(of(9));
        assertThat(of(8)).isLessThan(of(11));
        assertThat(of(8)).isLessThan(of(13));
    }

    @Test(expected = IllegalVersionException.class)
    public void lessOrEqual_fail() throws Exception {
        assertThat(of(8)).isLessOrEqual(of(7));
    }

    @Test
    public void lessOrEqual() throws Exception {
        assertThat(of(8)).isLessOrEqual(of(8));
        assertThat(of(8)).isLessOrEqual(of(9));
        assertThat(of(8)).isLessOrEqual(of(11));
    }

    @Test(expected = IllegalVersionException.class)
    public void greaterThan_fail_equal() throws Exception {
        assertThat(of(8)).isGreaterThan(of(8));
    }

    @Test(expected = IllegalVersionException.class)
    public void greaterThan_fail_less() throws Exception {
        assertThat(of(8)).isGreaterThan(of(9));
    }

    @Test
    public void greaterThan() throws Exception {
        assertThat(of(8)).isGreaterThan(of(3));
        assertThat(of(8)).isGreaterThan(of(7));
    }

    @Test(expected = IllegalVersionException.class)
    public void greaterOrEqual_fail() throws Exception {
        assertThat(of(8)).isGreaterOrEqual(of(9));
    }

    @Test
    public void greaterOrEqual() throws Exception {
        assertThat(of(8)).isGreaterOrEqual(of(1));
        assertThat(of(8)).isGreaterOrEqual(of(7));
        assertThat(of(8)).isGreaterOrEqual(of(8));
    }

    @Test
    public void equalTo() throws Exception {
        assertThat(of(8)).isEqualTo(of(8));
    }

    @Test(expected = IllegalVersionException.class)
    public void equalTo_smaller() throws Exception {
        assertThat(of(8)).isEqualTo(of(2));
    }

    @Test(expected = IllegalVersionException.class)
    public void equalTo_greater() throws Exception {
        assertThat(of(8)).isEqualTo(of(11));
    }

    @Test(expected = IllegalVersionException.class)
    public void isUnknown_fail() throws Exception {
        assertThat(of(8)).isUnknown();
    }

    @Test
    public void isUnknown() throws Exception {
        assertThat(of(-1)).isUnknown();
        assertThat(Version.UNKNOWN).isUnknown();
    }

    @Test(expected = IllegalVersionException.class)
    public void isNotUnknown_fail() throws Exception {
        assertThat(Version.UNKNOWN).isNotUnknown();
    }

    @Test(expected = IllegalVersionException.class)
    public void isNotUnknown_fail_explicit() throws Exception {
        assertThat(of(-1)).isNotUnknown();
    }

    @Test
    public void isNotUnknown() throws Exception {
        assertThat(of(8)).isNotUnknown();
    }

}