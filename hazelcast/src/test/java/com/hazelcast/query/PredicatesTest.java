package com.hazelcast.query;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import com.hazelcast.core.MapEntry;

public class PredicatesTest {
    @Test
    public void testEqual() {
        assertFalse(Predicates.equal(new DummyExpression("value1"), "value").apply(null));
        assertFalse(Predicates.equal(new DummyExpression("value"), "value1").apply(null));
        assertTrue(Predicates.equal(new DummyExpression("value"), "value").apply(null));
        assertTrue(Predicates.greaterThan(new DummyExpression(6), 5).apply(null));
        assertFalse(Predicates.greaterThan(new DummyExpression(4), 5).apply(null));
        assertFalse(Predicates.greaterThan(new DummyExpression(5), 5).apply(null));
        assertTrue(Predicates.greaterEqual(new DummyExpression(5), 5).apply(null));
        assertTrue(Predicates.lessThan(new DummyExpression(6), 7).apply(null));
        assertFalse(Predicates.lessThan(new DummyExpression(4), 3).apply(null));
        assertFalse(Predicates.lessThan(new DummyExpression(4), 4).apply(null));
        assertTrue(Predicates.lessEqual(new DummyExpression(4), 4).apply(null));
        assertTrue(Predicates.between(new DummyExpression(5), 4, 6).apply(null));
        assertTrue(Predicates.between(new DummyExpression(5), 5, 6).apply(null));
        assertFalse(Predicates.between(new DummyExpression(5), 6, 7).apply(null));
        
    }

    class DummyExpression implements Expression{
        Object value;

        DummyExpression(Object value) {
            this.value = value;
        }

        public Object getValue(Object obj) {
            return value;
        }
    }

    static MapEntry create(final Object key, final Object value) {
        return new MapEntry() {
            public long getCost() {
                return 0;
            }

            public long getCreationTime() {
                return 0;
            }

            public long getExpirationTime() {
                return 0;
            }

            public int getHits() {
                return 0;
            }

            public long getLastAccessTime() {
                return 0;
            }

            public long getLastUpdateTime() {
                return 0;
            }

            public long getVersion() {
                return 0;
            }

            public boolean isValid() {
                return true;
            }

            public Object getKey() {
                return key;
            }

            public Object getValue() {
                return value;
            }

            public Object setValue(Object value) {
                return value;
            }
        };
    }
}
