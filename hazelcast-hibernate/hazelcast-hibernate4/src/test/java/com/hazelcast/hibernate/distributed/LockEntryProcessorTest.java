package com.hazelcast.hibernate.distributed;

import com.hazelcast.hibernate.serialization.Expirable;
import com.hazelcast.hibernate.serialization.ExpiryMarker;
import com.hazelcast.hibernate.serialization.Value;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.Matchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LockEntryProcessorTest {

    @Test
    public void testProcessWithNullEntry() throws Exception {
        Map.Entry<Object, Expirable> entry = mockEntry(null);
        LockEntryProcessor processor = new LockEntryProcessor("next-marker-id", 100L, null);
        ArgumentCaptor<Expirable> captor = ArgumentCaptor.forClass(Expirable.class);
        processor.process(entry);
        verify(entry).setValue(captor.capture());
        Expirable result = captor.getValue();
        assertThat(result, instanceOf(ExpiryMarker.class));
        ExpiryMarker marker = (ExpiryMarker) result;
        assertTrue(marker.matches(new ExpiryMarker(null, 0L, "next-marker-id")));
    }

    @Test
    public void testProcessWithValueEntry() throws Exception {
        Map.Entry<Object, Expirable> entry = mockEntry(new Value(null, 500L, "blah"));
        LockEntryProcessor processor = new LockEntryProcessor("next-marker-id", 100L, null);
        ArgumentCaptor<Expirable> captor = ArgumentCaptor.forClass(Expirable.class);
        processor.process(entry);
        verify(entry).setValue(captor.capture());
        Expirable result = captor.getValue();
        assertThat(result, instanceOf(ExpiryMarker.class));
        ExpiryMarker marker = (ExpiryMarker) result;
        assertTrue(marker.matches(new ExpiryMarker(null, 0L, "next-marker-id")));
    }

    @Test
    public void testProcessWithMarkerEntry() throws Exception {
        ExpiryMarker original = new ExpiryMarker(null, 0L, "the-marker-id");
        Map.Entry<Object, Expirable> entry = mockEntry(original);
        LockEntryProcessor processor = new LockEntryProcessor("next-marker-id", 100L, null);
        ArgumentCaptor<Expirable> captor = ArgumentCaptor.forClass(Expirable.class);
        processor.process(entry);
        verify(entry).setValue(captor.capture());
        Expirable result = captor.getValue();
        assertThat(result, instanceOf(ExpiryMarker.class));
        ExpiryMarker marker = (ExpiryMarker) result;
        assertFalse(marker.matches(new ExpiryMarker(null, 0L, "next-marker-id")));
        assertTrue(marker.matches(original));
        assertTrue(marker.isConcurrent());
    }

    @SuppressWarnings("unchecked")
    private Map.Entry<Object, Expirable> mockEntry(Expirable value) {
        Map.Entry entry = mock(Map.Entry.class);
        when(entry.getValue()).thenReturn(value);
        return entry;
    }

}