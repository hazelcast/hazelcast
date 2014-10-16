

### Paging Predicate (Order & Limit)

Hazelcast provides paging for defined predicates. For this purpose, `PagingPredicate` class has been developed. You may want to
get collection of keys, values or entries page by page, by filtering them with predicates and giving the size of pages. Also, you
can sort the entries by specifying comparators.

Below is a sample code where the `greaterEqual` predicate is used to get values from "students" map. This predicate puts a filter
such that the objects with value of "age" is greater than or equal to 18 will be retrieved. Then, a `PagingPredicate` is
constructed in which the page size is 5. So, there will be 5 objects in each page.

The first time the values are called will constitute the first page. You can get the subsequent pages by using the `nextPage()`
method of `PagingPredicate` and querying the map again with updated `PagingPredicate`.


```java
IMap<Integer, Student> map = hazelcastInstance.getMap( "students" );
Predicate greaterEqual = Predicates.greaterEqual( "age", 18 );
PagingPredicate pagingPredicate = new PagingPredicate( greaterEqual, 5 );
// Retrieve the first page
Collection<Student> values = map.values( pagingPredicate );
...
// Set up next page
pagingPredicate.nextPage();
// Retrieve next page
values = map.values( pagingPredicate );
...
```

If a comparator is not specified for `PagingPredicate` and when you want to get collection of keys or values page by page, this collection must be an instance of `Comparable` (i.e. it must implement `java.lang.Comparable`). Otherwise, `java.lang.IllegalArgument` exception is thrown.

Paging Predicate is not supported in Transactional Context.
<br></br>

***RELATED INFORMATION***

*Please refer to the [Javadoc](http://hazelcast.org/docs/latest/javadoc/com/hazelcast/query/Predicates.html) for all
predicates.*



