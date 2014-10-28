package com.hazelcast.hibernate.serialization;

import org.hibernate.internal.util.compare.ComparableComparator;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExpiryMarkerTest {

    @Test
    public void testIsReplaceableByTimestampBeforeTimeout() throws Exception {
        ExpiryMarker marker = new ExpiryMarker(null, 100L, "the-marker-id");
        assertFalse("marker is not replaceable when it hasn't timed out", marker.isReplaceableBy(99L, null, null));
    }

    @Test
    public void testIsReplaceableByTimestampEqualTimeout() throws Exception {
        ExpiryMarker marker = new ExpiryMarker(null, 100L, "the-marker-id");
        assertFalse("marker is not replaceable when it hasn't timed out", marker.isReplaceableBy(100L, null, null));
    }

    @Test
    public void testIsReplaceableByTimestampAfterTimeout() throws Exception {
        ExpiryMarker marker = new ExpiryMarker(null, 100L, "the-marker-id");
        assertTrue("marker is replaceable when it has timed out", marker.isReplaceableBy(101L, null, null));
    }

    @Test
    public void testIsReplaceableByTimestampBeforeExpiredTimestamp() throws Exception {
        ExpiryMarker marker = new ExpiryMarker(null, 150L, "the-marker-id").expire(100L);
        assertFalse("marker is not replaceable when it when timestamp before expiry",
                marker.isReplaceableBy(99L, null, null));
    }

    @Test
    public void testIsReplaceableByTimestampEqualExpiredTimestamp() throws Exception {
        ExpiryMarker marker = new ExpiryMarker(null, 150L, "the-marker-id").expire(100L);
        assertFalse("marker is not replaceable when it when timestamp equal to expiry",
                marker.isReplaceableBy(100L, null, null));
    }

    @Test
    public void testIsReplaceableByTimestampAfterExpiredTimestamp() throws Exception {
        ExpiryMarker marker = new ExpiryMarker(null, 150L, "the-marker-id").expire(100L);
        assertTrue("marker is replaceable when it when timestamp after expiry",
                marker.isReplaceableBy(101L, null, null));
    }

    @Test
    public void testIsReplaceableByVersionBefore() throws Exception {
        ExpiryMarker marker = new ExpiryMarker(10, 150L, "the-marker-id").expire(100L);
        assertFalse("marker is replaceable when it when version before",
                marker.isReplaceableBy(101L, 9, ComparableComparator.INSTANCE));
    }

    @Test
    public void testIsReplaceableByVersionEqual() throws Exception {
        ExpiryMarker marker = new ExpiryMarker(10, 150L, "the-marker-id").expire(100L);
        assertFalse("marker is replaceable when it when version equal",
                marker.isReplaceableBy(101L, 10, ComparableComparator.INSTANCE));
    }

    @Test
    public void testIsReplaceableByVersionAfter() throws Exception {
        ExpiryMarker marker = new ExpiryMarker(10, 150L, "the-marker-id").expire(100L);
        assertTrue("marker is replaceable when it when version after",
                marker.isReplaceableBy(99L, 11, ComparableComparator.INSTANCE));
    }

    @Test
    public void testMatchesOnlyUsesMarkerId() throws Exception {
        ExpiryMarker marker = new ExpiryMarker(10, 150L, "the-marker-id");
        assertTrue(marker.matches(marker));
        assertTrue(marker.matches(marker.expire(100)));
        assertTrue(marker.matches(marker.markForExpiration(100L, "some-other-marker-id")));
        assertTrue(marker.matches(new ExpiryMarker(9, 100L, "the-marker-id")));
        assertFalse(marker.matches(new ExpiryMarker(10, 150L, "some-other-marker-id")));
    }
}