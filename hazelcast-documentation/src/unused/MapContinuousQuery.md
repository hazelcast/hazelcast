
### Continuous Query

You can listen map entry events providing a predicate and so, event will be fired for each entry validated by your query. IMap has a single method for listening map providing query.

```java
/**
 * Adds an continuous entry listener for this map. Listener will get notified
 * for map add/remove/update/evict events filtered by given predicate.
 *
 * @param listener  entry listener
 * @param predicate predicate for filtering entries
 */
void addEntryListener(EntryListener<K, V> listener, Predicate<K, V> predicate, K key, boolean includeValue);
```
