package com.hazelcast.hibernate.distributed;

import com.hazelcast.hibernate.serialization.Expirable;
import com.hazelcast.hibernate.serialization.ExpiryMarker;
import com.hazelcast.hibernate.serialization.Value;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
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
