package com.hazelcast.jet;

import com.hazelcast.jet.Distributed.Optional;
import com.hazelcast.jet.TestUtil.DummyUncheckedTestException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.NoSuchElementException;

import static com.hazelcast.jet.Distributed.Optional.empty;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OptionalTest {

    private Optional<Integer> optional = Optional.of(123456);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testIfPresent() {
        optional.ifPresent(v -> assertEquals(v, optional.get()));
    }

    @Test
    public void testFilterOrElseGet() {
        assertEquals(optional.get(), optional.orElseGet(() -> 10));
        assertEquals(10, empty().orElseGet(() -> 10));
    }

    @Test
    public void testOrElseThrow() {
        DummyUncheckedTestException expectedExc = new DummyUncheckedTestException();
        exception.expect(equalTo(expectedExc));
        empty().orElseThrow(() -> expectedExc);
    }

    @Test
    public void testFilter() {
        // not-null to empty
        assertFalse(optional.filter(v -> v == 0).isPresent());

        // empty to empty
        assertFalse(empty().filter(v -> false).isPresent());
        assertFalse(empty().filter(v -> true).isPresent());

        // not-null to not-null
        assertEquals(optional.get(), optional.filter(v -> true).get());
    }

    @Test
    public void testMap() {
        assertEquals(String.valueOf(optional.get()), optional.map(String::valueOf).get());
    }

    @Test
    public void testHashCode() {
        assertEquals(optional.get().hashCode(), optional.hashCode());
    }

    @Test
    public void testEquals() {
        assertTrue(optional.equals(optional));
        assertFalse(optional.equals("blabla"));
        assertTrue(empty().equals(Distributed.Optional.ofNullable(null)));
        assertTrue(Distributed.Optional.of(5).equals(Distributed.Optional.ofNullable(5)));
    }

    @Test
    public void testFlatMap() {
        // not-null to empty
        assertFalse(optional.flatMap(v -> empty()).isPresent());
        // empty to not-null
        assertFalse(empty().flatMap(v -> optional).isPresent());
        // empty to empty
        assertFalse(empty().flatMap(v -> empty()).isPresent());
        // not-null to not-null
        assertEquals(Integer.valueOf(5), optional.flatMap(v -> Distributed.Optional.of(5)).get());
    }

    @Test
    public void when_getOnEmpty_then_noSuchElementException() {
        exception.expect(NoSuchElementException.class);
        empty().get();
    }
}
