package com.hazelcast.hibernate.distributed;

import com.hazelcast.hibernate.serialization.Expirable;
import com.hazelcast.hibernate.serialization.ExpiryMarker;
import com.hazelcast.hibernate.serialization.Value;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class UpdateEntryProcessorTest {

    @Test
    public void testProcessWithNullEntry() throws Exception {
        Map.Entry<Object, Expirable> entry = mockEntry(null);
        UpdateEntryProcessor processor = new UpdateEntryProcessor(new ExpiryMarker(null, 100L, "the-marker-id"), "new-value", null, "next-marker-id", 150L);
        ArgumentCaptor<Expirable> captor = ArgumentCaptor.forClass(Expirable.class);
        assertTrue(processor.process(entry));
        verify(entry).setValue(captor.capture());
        Expirable result = captor.getValue();
        assertThat(result, instanceOf(Value.class));
        assertEquals("new-value", result.getValue());
    }

    @Test
    public void testProcessWithMatchingMarker() throws Exception {
        Map.Entry<Object, Expirable> entry = mockEntry(new ExpiryMarker(null, 100L, "the-marker-id"));
        UpdateEntryProcessor processor = new UpdateEntryProcessor(new ExpiryMarker(null, 100L, "the-marker-id"), "new-value", null, "next-marker-id", 150L);
        ArgumentCaptor<Expirable> captor = ArgumentCaptor.forClass(Expirable.class);
        assertTrue(processor.process(entry));
        verify(entry).setValue(captor.capture());
        Expirable result = captor.getValue();
        assertThat(result, instanceOf(Value.class));
        assertEquals("new-value", result.getValue());
    }

    @Test
    public void testProcessWithMatchingConcurrentMarker() throws Exception {
        Map.Entry<Object, Expirable> entry = mockEntry(new ExpiryMarker(null, 100L, "the-marker-id").markForExpiration(100L, "ignored"));
        UpdateEntryProcessor processor = new UpdateEntryProcessor(new ExpiryMarker(null, 100L, "the-marker-id"), "new-value", null, "next-marker-id", 150L);
        ArgumentCaptor<Expirable> captor = ArgumentCaptor.forClass(Expirable.class);
        assertFalse(processor.process(entry));
        verify(entry).setValue(captor.capture());
        Expirable result = captor.getValue();
        assertThat(result, instanceOf(ExpiryMarker.class));
    }

    @Test
    public void testProcessWithDifferentMarker() throws Exception {
        Map.Entry<Object, Expirable> entry = mockEntry(new ExpiryMarker(null, 100L, "other-marker-id"));
        UpdateEntryProcessor processor = new UpdateEntryProcessor(new ExpiryMarker(null, 100L, "the-marker-id"), "new-value", null, "next-marker-id", 150L);
        assertFalse(processor.process(entry));
        verify(entry, never()).setValue(any(Expirable.class));
    }

    @Test
    public void testProcessWithValue() throws Exception {
        Map.Entry<Object, Expirable> entry = mockEntry(new Value(null, 100L, "some-value"));
        UpdateEntryProcessor processor = new UpdateEntryProcessor(new ExpiryMarker(null, 100L, "the-marker-id"), "new-value", null, "next-marker-id", 150L);
        ArgumentCaptor<Expirable> captor = ArgumentCaptor.forClass(Expirable.class);
        assertFalse(processor.process(entry));
        verify(entry).setValue(captor.capture());
        Expirable result = captor.getValue();
        assertThat(result, instanceOf(ExpiryMarker.class));
        assertTrue("Marker should be expired", result.isReplaceableBy(151L, null, null));
    }

    @SuppressWarnings("unchecked")
    private Map.Entry<Object, Expirable> mockEntry(Expirable value) {
        Map.Entry entry = mock(Map.Entry.class);
        when(entry.getValue()).thenReturn(value);
        return entry;
    }

}
