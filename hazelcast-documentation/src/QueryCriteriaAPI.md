


### Criteria API

Criteria API is a programming interface offered by Hazelcast similar to Java Persistence Query Language (JPQL). Below is the code
for the above sample query.

```java
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.EntryObject;
import com.hazelcast.config.Config;

IMap<String, Employee> map = hazelcastInstance.getMap( "employee" );

EntryObject e = new PredicateBuilder().getEntryObject();
Predicate predicate = e.is( "active" ).and( e.get( "age" ).lessThan( 30 ) );

Set<Employee> employees = map.values( predicate );
```

In the above sample, `predicate` verifies whether the entry is active and its `age` value is less than 30. This `predicate` is
applied to the `employee` map using the `map.values(predicate)` method. This method sends the predicate to all cluster members
and merges the results coming from them. As you can guess, since the predicate is communicated between the members, it needs to
be serializable.

***NOTE:*** *Predicates can also be applied to `keySet`, `entrySet` and `localKeySet` of Hazelcast distributed map.*

#### Predicates Class

`Predicates` class offered by Hazelcast includes a lot of operators that will meet your query requirements. Some of them are
explained below.

- *equal*: checks if the result of an expression is equal to a given value.
- *notEqual*: checks if the result of an expression is not equal to a given value.
- *instanceOf*: checks if the result of an expression has a certain type
- *like*: checks if the result of an expression matches some string pattern. % (percentage sign) is placeholder for many
characters,  (underscore) is placeholder for only one character.
- *greaterThan*: checks if the result of an expression is greater than a certain value.
- *greaterEqual*: checks if the result of an expression is greater or equal than a certain value.
- *lessThan*: checks if the result of an expression is less than a certain value
- *lessEqual*: checks if the result of an expression is than than or equal to a certain value.
- *between*: checks if the result of an expression is between 2 values (this is inclusive).
- *in*: checks if the result of an expression is an element of a certain collection.
- *isNot*: checks if the result of an expression is false.
- *regex*: checks if the result of an expression matches some regular expression.
<br></br>

***RELATED INFORMATION***

*Please see
[Predicates](https://github.com/hazelcast/hazelcast/blob/2709bc81cd499a3160827de24422cdb6cf98fe36/hazelcast/src/main/java/com/hazelcast/query/Predicates.java)
class for all predicates provided.*


#### Joining Predicates with AND, OR, NOT

Predicates can be joined using the `and`, `or` and `not` operators, as shown in the below examples.

```java
public Set<Person> getWithNameAndAge( String name, int age ) {
  Predicate namePredicate = Predicates.equal( "name", name );
  Predicate agePredicate = Predicates.equal( "age", age );
  Predicate predicate = Predicates.and( namePredicate, agePredicate );
  return personMap.values( predicate );
}
```

```java
public Set<Person> getWithNameOrAge( String name, int age ) {
  Predicate namePredicate = Predicates.equal( "name", name );
  Predicate agePredicate = Predicates.equal( "age", age );
  Predicate predicate = Predicates.or( namePredicate, agePredicate );
  return personMap.values( predicate );
}
```

```java
public Set<Person> getNotWithName( String name ) {
  Predicate namePredicate = Predicates.equal( "name", name );
  Predicate predicate = Predicates.not( namePredicate );
  return personMap.values( predicate );
}
```


#### PredicateBuilder

Predicate usage can be simplified using the `PredicateBuilder` class. It offers a more simpler predicate building. Please see the
below sample code which which selects all people with a certain name and age.

```java
public Set<Person> getWithNameAndAgeSimplified( String name, int age ) {
  EntryObject e = new PredicateBuilder().getEntryObject();
  Predicate agePredicate = e.get( "age" ).equal( age );
  Predicate predicate = e.get( "name" ).equal( name ).and( agePredicate );
  return personMap.values( predicate );
}
```


