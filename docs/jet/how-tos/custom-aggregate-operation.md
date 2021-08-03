---
title: Build a Custom Aggregate Operation
description: How to create a custom aggregation operation using Jet
---

One of the most important kinds of processing Jet does is aggregation. In
general it is a transformation of a set of input values into a single
output value. The function that does this transformation is called the
_aggregate function_. A basic example is `sum` applied to a set of
integer numbers, but the result can also be a complex value, for example
a list of all the input items.

Jet's library contains a range of [predefined aggregate
functions](/javadoc/{jet-version}/com/hazelcast/jet/aggregate/AggregateOperations.html),
but it also exposes an abstraction, called
[`AggregateOperation`](/javadoc/{jet-version}/com/hazelcast/jet/aggregate/AggregateOperation.html),
that allows you to plug in your own. Since Jet does the aggregation in a
parallelized and distributed way, you can't simply supply a piece of
Java code that implements the aggregate function; we need you to break
it down into several smaller pieces that fit into Jet's processing
engine.

## Characteristics of Distributed Aggregation

The ability to compute the aggregate function in parallel comes at a
cost: Jet must be able to give a slice of the total data set to each
processing unit and then combine the partial results from all the units.
The combining step is crucial: it will only make sense if we're
combining the partial results of a _commutative associative_ function
(CA for short). On the example of `sum` this is trivial: we know from
elementary school that `+` is a CA operation. If you have a stream of
numbers: `{17, 37, 5, 11, 42}`, you can sum up `{17, 5}` separately from
`{42, 11, 37}` and then combine the partial sums (also note the
reordering of the elements).

If you need something more complex, like `average`, it doesn't by itself
have this property; however if you add one more ingredient, the `finish`
function, you can express it easily. Jet allows you to first compute
some CA function, whose partial results can be combined, and then at the
very end apply the `finish` function on the fully combined result. To
compute the average, your CA function will output the pair `(sum,
count)`. Two such pairs are trivial to combine by summing each
component. The `finish` function will be `sum / count`.

In addition to the mathematical side, there is also the practical one:
you have to provide Jet with a specific mutable object, called the
`accumulator`, which will keep the `running score` of the operation in
progress. For the `average` example, it would be something like

```java
public class AvgAccumulator {

    private long sum;
    private long count;

    public void accumulate(long value) {
        sum += value;
        count++;
    }

    public void combine(AvgAccumulator that) {
        this.sum += that.sum;
        this.count += that.sum;
    }

    public double finish() {
        return (double) sum / count;
    }
}
```

This object will also have to be [serializable](../api/serialization),
and preferably with Hazelcast's serialization instead of Java's because
in a group-and-aggregate operation there's one accumulator per each key
and all of them have to be sent across the network to be combined and
finished.

## The Building Blocks

Instead of requiring you to write a complete class from scratch, Jet
separates the concern of holding the accumulated state from that of the
computation performed on it. This means that you just need one
accumulator class for each kind of structure that holds the accumulated
data, as opposed to one for each aggregate operation. Jet's library
offers in the
[`com.hazelcast.jet.accumulator`](/javadoc/{jet-version}/com/hazelcast/jet/accumulator/package-summary.html)
package several such classes, one of them being
[`LongLongAccumulator`](/javadoc/{jet-version}/com/hazelcast/jet/accumulator/LongLongAccumulator.html),
which is a match for our `average` function. You'll just have to supply
the logic on top of it.

Specifically, you have to provide a set of six functions (we call them
"`primitives`"):

- `create` a new accumulator object.
- `accumulate` the data of an item by mutating the accumulator's state.
- `combine` the contents of the right-hand accumulator into the
  left-hand one.
- `deduct` the contents of the right-hand accumulator from the left-hand
  one (undo the effects of `combine`).
- `finish` accumulation by transforming the accumulator object into the
  final result.
- `export` the result of aggregation in a way that's not destructive for
  the accumulator (used in rolling aggregations).

We already mentioned most of these above. The `deduct` primitive is
optional and Jet can manage without it, but if you are computing a
sliding window over an infinite stream, this primitive can give a
significant performance boost because it allows Jet to reuse the results
of the previous calculations.

In a similar fashion Jet discerns between the `export` and `finish`
primitives for optimization purposes. Every function that works as the
`export` primitive will also work as `finish`, but you can specify a
different `finish` that reuses the state already allocated in the
accumulator. Jet applies `finish` only if it will never again use that
accumulator.

If you happen to have a deeper familiarity with JDK's `java.util.stream`
API, you'll find `AggregateOperation` quite similar to
`java.util.stream.Collector`, which is also a holder of several
functional primitives. Jet's definitions are slightly different, though,
and there are the additional optimizing primitives we just mentioned.

Let's see how this works with our `average` function. Using
`LongLongAccumulator` we can express our `accumulate` primitive as

```java
(acc, n) -> {
    acc.set1(acc.get1() + n);
    acc.set2(acc.get2() + 1);
}
```

The `export`/`finish` primitive will be

```java
acc -> (double) acc.get1() / acc.get2()
```

Now we have to define the other three primitives to match our main
logic. For `create` we just refer to the constructor:
`LongLongAccumulator::new`. The `combine` primitive expects you to
update the left-hand accumulator with the contents of the right-hand
one, so:

```java
(left, right) -> {
    left.set1(left.get1() + right.get1());
    left.set2(left.get2() + right.get2());
}
```

Deducting must undo the effect of a previous `combine`:

```java
(left, right) -> {
    left.set1(left.get1() - right.get1());
    left.set2(left.get2() - right.get2());
}
```

All put together, we can define our averaging operation as follows:

```java
AggregateOperation1<Long, LongLongAccumulator, Double> aggrOp = AggregateOperation
    .withCreate(LongLongAccumulator::new)
    .<Long>andAccumulate((acc, n) -> {
        acc.set1(acc.get1() + n);
        acc.set2(acc.get2() + 1);
    })
    .andCombine((left, right) -> {
        left.set1(left.get1() + right.get1());
        left.set2(left.get2() + right.get2());
    })
    .andDeduct((left, right) -> {
        left.set1(left.get1() - right.get1());
        left.set2(left.get2() - right.get2());
    })
    .andExportFinish(acc -> (double) acc.get1() / acc.get2());
```

Let's stop for a second to look at the type we got:
`AggregateOperation1<Long, LongLongAccumulator, Double>`. Its type
parameters are:

1. `Long`: the type of the input item
2. `LongLongAccumulator`: the type of the accumulator
3. `Double`: the type of the result

Specifically note the `1` at the end of the type's name: it signifies
that it's the specialization of the general `AggregateOperation` to
exactly one input stream. In Hazelcast Jet you can also perform a
[co-aggregating](../api/stateful-transforms#co-group--join)
operation, aggregating several input streams together. Since the number
of input types is variable, the general `AggregateOperation` type cannot
statically capture them and we need separate subtypes. We decided to
statically support up to three input types; if you need more, you'll
have to resort to the less type-safe, general `AggregateOperation`.

## Aggregating Over Multiple Inputs

Hazelcast Jet can join several streams and simultaneously perform
aggregation on all of them. You specify a separate aggregate operation
for each input stream and have the opportunity to combine their results
when done. You can use aggregate operations [provided in the
library](/javadoc/{jet-version}/com/hazelcast/jet/aggregate/AggregateOperations.html)
(see the section on
[co-aggregating](../api/stateful-transforms#co-group--join) for an
example).

If you cannot express your aggregation logic using this approach, you
can also specify a custom multi-input aggregate operation that can
combine the items into the accumulator immediately as it receives them.

We'll present a simple example on how to build a custom multi-input
aggregate operation. Note that the same logic can also be expressed
using separate single-input operations; the point of the example is
introducing the API.

Say we are interested in the behavior of users in an online shop
application and want to gather the following statistics for each user:

1. total load time of the visited product pages
2. quantity of items added to the shopping cart
3. amount paid for bought items

This data is dispersed among separate datasets: `PageVisit`, `AddToCart`
and `Payment`. Note that in each case we're dealing with a simple `sum`
applied to a field in the input item. We can perform a
cogroup-and-aggregate transform with the following aggregate operation:

```java
Pipeline p = Pipeline.create();
BatchStage<PageVisit> pageVisit = p.readFrom(Sources.list("pageVisit"));
BatchStage<AddToCart> addToCart = p.readFrom(Sources.list("addToCart"));
BatchStage<Payment> payment = p.readFrom(Sources.list("payment"));

AggregateOperation3<PageVisit, AddToCart, Payment, LongAccumulator[], long[]>
    aggrOp = AggregateOperation
        .withCreate(() -> new LongAccumulator[] {
            new LongAccumulator(),
            new LongAccumulator(),
            new LongAccumulator()
        })
        .<PageVisit>andAccumulate0((accs, pv) -> accs[0].add(pv.loadTime()))
        .<AddToCart>andAccumulate1((accs, atc) -> accs[1].add(atc.quantity()))
        .<Payment>andAccumulate2((accs, pm) -> accs[2].add(pm.amount()))
        .andCombine((accs1, accs2) -> {
            accs1[0].add(accs2[0]);
            accs1[1].add(accs2[1]);
            accs1[2].add(accs2[2]);
        })
        .andExportFinish(accs -> new long[] {
            accs[0].get(),
            accs[1].get(),
            accs[2].get()
        });

BatchStage<Entry<Integer, long[]>> coGrouped =
    pageVisit.groupingKey(PageVisit::userId)
             .aggregate3(
                addToCart.groupingKey(AddToCart::userId),
                payment.groupingKey(Payment::userId),
                aggrOp
             );
```

Note how we got an `AggregateOperation3` and how it captured each input
type. When we use it as an argument to a cogroup-and-aggregate
transform, the compiler will ensure that the `ComputeStage`s we attach
to it have the correct type and are in the correct order.

On the other hand, if you use the co-aggregation
builder object, you'll construct the aggregate operation by calling
`andAccumulate(tag, accFn)` with all the tags you got from the
co-aggregation builder, and the static type will be just
`AggregateOperation`. The compiler won't be able to match up the inputs
to their treatment in the aggregate operation.
