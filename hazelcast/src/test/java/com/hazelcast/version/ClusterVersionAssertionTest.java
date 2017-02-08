package com.hazelcast.version;

import org.junit.Test;

import static com.hazelcast.version.ClusterVersion.of;
import static com.hazelcast.version.ClusterVersionAssertion.assertThat;

public class ClusterVersionAssertionTest {

    @Test(expected = IllegalVersionException.class)
    public void lessThan_fail_equal() throws Exception {
        assertThat(of("3.8")).isLessThan(of("3.8"));
    }

    @Test(expected = IllegalVersionException.class)
    public void lessThan_fail_less() throws Exception {
        assertThat(of("3.8")).isLessThan(of("3.7"));
    }

    @Test(expected = IllegalVersionException.class)
    public void lessThan_fail_lessMajor() throws Exception {
        assertThat(of("3.8")).isLessThan(of("2.7"));
    }

    @Test
    public void lessThan() throws Exception {
        assertThat(of("3.8")).isLessThan(of("3.9"));
        assertThat(of("3.8")).isLessThan(of("4.1"));
        assertThat(of("3.8")).isLessThan(of("4.8"));
    }

    @Test(expected = IllegalVersionException.class)
    public void lessOrEqual_fail() throws Exception {
        assertThat(of("3.8")).isLessOrEqual(of("3.7"));
    }

    @Test(expected = IllegalVersionException.class)
    public void lessOrEqual_fail_lessMajor() throws Exception {
        assertThat(of("3.8")).isLessOrEqual(of("2.7"));
    }

    @Test
    public void lessOrEqual() throws Exception {
        assertThat(of("3.8")).isLessOrEqual(of("3.8"));
        assertThat(of("3.8")).isLessOrEqual(of("3.9"));
        assertThat(of("3.8")).isLessOrEqual(of("4.1"));
        assertThat(of("3.8")).isLessOrEqual(of("4.8"));
    }

    @Test(expected = IllegalVersionException.class)
    public void greaterThan_fail_equal() throws Exception {
        assertThat(of("3.8")).isGreaterThan(of("3.8"));
    }

    @Test(expected = IllegalVersionException.class)
    public void greaterThan_fail_less() throws Exception {
        assertThat(of("3.8")).isGreaterThan(of("3.9"));
    }

    @Test(expected = IllegalVersionException.class)
    public void greaterThan_fail_greatedMajor() throws Exception {
        assertThat(of("3.8")).isGreaterThan(of("4.1"));
    }

    @Test
    public void greaterThan() throws Exception {
        assertThat(of("3.8")).isGreaterThan(of("2.7"));
        assertThat(of("3.8")).isGreaterThan(of("3.3"));
        assertThat(of("3.8")).isGreaterThan(of("3.7"));
    }

    @Test(expected = IllegalVersionException.class)
    public void greaterOrEqual_fail() throws Exception {
        assertThat(of("3.8")).isGreaterOrEqual(of("3.9"));
    }

    @Test(expected = IllegalVersionException.class)
    public void greaterOrEqual_fail_greatedMajor() throws Exception {
        assertThat(of("3.8")).isGreaterOrEqual(of("4.1"));
    }

    @Test
    public void greaterOrEqual() throws Exception {
        assertThat(of("3.8")).isGreaterOrEqual(of("2.7"));
        assertThat(of("3.8")).isGreaterOrEqual(of("3.1"));
        assertThat(of("3.8")).isGreaterOrEqual(of("3.7"));
        assertThat(of("3.8")).isGreaterOrEqual(of("3.8"));
    }

    @Test
    public void equalTo() throws Exception {
        assertThat(of("3.8")).isEqualTo(of("3.8"));
    }

    @Test(expected = IllegalVersionException.class)
    public void equalTo_smaller() throws Exception {
        assertThat(of("3.8")).isEqualTo(of("2.7"));
    }

    @Test(expected = IllegalVersionException.class)
    public void equalTo_greater() throws Exception {
        assertThat(of("3.8")).isEqualTo(of("4.7"));
    }

    @Test(expected = IllegalVersionException.class)
    public void isUnknown_fail() throws Exception {
        assertThat(of("3.8")).isUnknown();
    }

    @Test
    public void isUnknown() throws Exception {
        assertThat(of("0.0")).isUnknown();
        assertThat(ClusterVersion.UNKNOWN).isUnknown();
    }

    @Test(expected = IllegalVersionException.class)
    public void isNotUnknown_fail() throws Exception {
        assertThat(ClusterVersion.UNKNOWN).isNotUnknown();
    }

    @Test(expected = IllegalVersionException.class)
    public void isNotUnknown_fail_explicit() throws Exception {
        assertThat(of("0.0")).isNotUnknown();
    }

    @Test
    public void isNotUnknown() throws Exception {
        assertThat(of("3.8")).isNotUnknown();
    }

}