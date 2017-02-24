package com.hazelcast.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class StringUtilTest extends HazelcastTestSupport {

    @Test
    public void getterIntoProperty_whenNull_returnNull() throws Exception {
        assertEquals("", StringUtil.getterIntoProperty(""));
    }

    @Test
    public void getterIntoProperty_whenEmpty_returnEmptyString() throws Exception {
        assertEquals("", StringUtil.getterIntoProperty(""));
    }

    @Test
    public void getterIntoProperty_whenGet_returnUnchanged() throws Exception {
        assertEquals("get", StringUtil.getterIntoProperty("get"));
    }

    @Test
    public void getterIntoProperty_whenGetFoo_returnFoo() throws Exception {
        assertEquals("foo", StringUtil.getterIntoProperty("getFoo"));
    }

    @Test
    public void getterIntoProperty_whenGetF_returnF() throws Exception {
        assertEquals("f", StringUtil.getterIntoProperty("getF"));
    }


    @Test
    public void getterIntoProperty_whenGetNumber_returnNumber() throws Exception {
        assertEquals("8", StringUtil.getterIntoProperty("get8"));
    }

    @Test
    public void getterIntoProperty_whenPropertyIsLowerCase_DoNotChange() throws Exception {
        assertEquals("getfoo", StringUtil.getterIntoProperty("getfoo"));
    }

}