package com.hazelcast.aws;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TagTest {
    @Test
    public void tagKeyOnly() {
        // given
        String key = "Key";
        String value = null;

        // when
        Tag tag = new Tag(key, value);

        // then
        assertEquals(tag.getKey(), key);
        assertNull(tag.getValue());
    }

    @Test
    public void tagValueOnly() {
        // given
        String key = null;
        String value = "Value";

        // when
        Tag tag = new Tag(key, value);

        // then
        assertNull(tag.getKey());
        assertEquals(tag.getValue(), value);
    }

    @Test(expected = IllegalArgumentException.class)
    public void missingKeyAndValue() {
        // given
        String key = null;
        String value = null;

        // when
        new Tag(key, value);

        // then
        // throws exception
    }
}
