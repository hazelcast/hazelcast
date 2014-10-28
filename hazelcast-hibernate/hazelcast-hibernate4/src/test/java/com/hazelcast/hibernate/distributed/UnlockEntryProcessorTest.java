package com.hazelcast.hibernate.distributed;

import com.hazelcast.hibernate.serialization.Expirable;
import com.hazelcast.hibernate.serialization.ExpiryMarker;
import com.hazelcast.hibernate.serialization.Value;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class UnlockEntryProcessorTest {

    @Test
    public void testProcessWithNullEntry() throws Exception {
        Map.Entry<Object, Expirable> entry = mockEntry(null);
        UnlockEntryProcessor processor = new UnlockEntryProcessor(new ExpiryMarker(null, 100L, "other-marker-id"), "next-marker-id", 100L);
        processor.process(entry);
        verify(entry, never()).setValue(any(Expirable.class));
    }

    @Test
    public void testProcessWithDifferentMarker() throws Exception {
        Map.Entry<Object, Expirable> entry = mockEntry(new ExpiryMarker(null, 100L, "the-marker-id"));
        UnlockEntryProcessor processor = new UnlockEntryProcessor(new ExpiryMarker(null, 100L, "other-marker-id"), "next-marker-id", 100L);
        processor.process(entry);
        verify(entry, never()).setValue(any(Expirable.class));
    }

    @Test
    public void testProcessWithValue() throws Exception {
        Map.Entry<Object, Expirable> entry = mockEntry(new Value(null, 100L, "some-value"));
        UnlockEntryProcessor processor = new UnlockEntryProcessor(new ExpiryMarker(null, 100L, "other-marker-id"), "next-marker-id", 150L);
        ArgumentCaptor<Expirable> captor = ArgumentCaptor.forClass(Expirable.class);
        processor.process(entry);
        verify(entry).setValue(captor.capture());
        Expirable result = captor.getValue();
        assertThat(result, instanceOf(ExpiryMarker.class));
        ExpiryMarker marker = (ExpiryMarker) result;
        assertFalse("market must be expirable after timestamp", marker.isReplaceableBy(150L, null, null));
        assertTrue("market must be expirable after timestamp", marker.isReplaceableBy(151L, null, null));
    }

    @Test
    public void testProcessMatchingLock() throws Exception {
        ExpiryMarker original = new ExpiryMarker(null, 100L, "the-marker-id").markForExpiration(100L, "some-marker-id");
        Map.Entry<Object, Expirable> entry = mockEntry(original);
        UnlockEntryProcessor processor = new UnlockEntryProcessor(new ExpiryMarker(null, 100L, "the-marker-id"), "next-marker-id", 150L);
        ArgumentCaptor<Expirable> captor = ArgumentCaptor.forClass(Expirable.class);
        processor.process(entry);
        verify(entry).setValue(captor.capture());
        Expirable result = captor.getValue();
        assertNotSame(original, result);
        assertThat(result, instanceOf(ExpiryMarker.class));
        ExpiryMarker marker = (ExpiryMarker) result;
        assertTrue(marker.matches(original));
    }

    @SuppressWarnings("unchecked")
    private Map.Entry<Object, Expirable> mockEntry(Expirable value) {
        Map.Entry entry = mock(Map.Entry.class);
        when(entry.getValue()).thenReturn(value);
        return entry;
    }

}