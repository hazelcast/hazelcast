---
title: Stateless Transforms
description: Purely functional data transformations available in Jet.
id: version-4.0-stateless-transforms
original_id: stateless-transforms
---

Stateless transforms are the bread and butter of a data pipeline: they
transform the input into the correct shape that is required by further,
more complex transforms. The key feature of these transforms is that
they do not have side-effects and they treat each item in isolation.

## map

Mapping is the simplest kind of stateless transformation. It simply
applies a function to the input item, and passes the output to the next
stage.

```java
StreamStage<String> names = stage.map(name -> name.toLowerCase());
```

## filter

Similar to `map`, the `filter` is a stateless operator that applies a
predicate to the input to decide whether to pass it to the output.

```java
BatchStage<String> names = stage.filter(name -> !name.isEmpty());
```

## flatMap

`flatMap` is equivalent to `map`, with the difference that instead of
one output item you can have arbitrary number of output items per input
item. The output type is a `Traverser`, which is a Jet type similar to
an `Iterator`. For example, the code below will split a sentence into
individual items consisting of words:

```java
StreamStage<String> words = stage.flatMap(
    sentence -> Traversers.traverseArray(sentence.split("\\W+"))
);
```

## merge

This transform merges the contents of two streams into one. The item
type in the right-hand stage must be the same or a subtype of the one in
the left-hand stage. The items from both sides will be interleaved in
arbitrary order.

```java
StreamStage<Trade> tradesNewYork = pipeline
    .readFrom(KafkaSources.kafka(.., "nyc"))
    .withoutTimestamps();
StreamStage<Trade> tradesTokyo = pipeline
    .readFrom(KafkaSources.kafka(.., "nyc"))
    .withoutTimestamps();
StreamStage<Trade> tradesNyAndTokyo = tradesNewYork.merge(tradesTokyo);
```

## mapUsingIMap

This transform looks up each incoming item from the corresponding
[IMap](data-structures) and the result of the lookup is combined with
the input item.

```java
StreamStage<Order> orders = pipeline
    .readFrom(KafkaSources.kafka(.., "orders"))
    .withoutTimestamps();
StreamStage<OrderDetails> details = orders.mapUsingIMap("products",
  order -> order.getProductId(),
  (order, product) -> new OrderDetails(order, product));
```

The above code can be thought of as equivalent to below, where the input
is of type `Order`

```java
public void getOrderDetails(Order order) {
    IMap<String, ProductDetails> map = jet.getMap("products");
    ProductDetails product = map.get(order.getProductId());
    return new OrderDetails(order, product);
}
```

See [Joining Static Data to a Stream](../tutorials/map-join) for a
tutorial using this operator.

## mapUsingReplicatedMap

This transform is equivalent to [mapUsingIMap](#mapUsingImap) with the
only difference that a [ReplicatedMap](data-structures) is used instead
of an `IMap`.

```java
StreamStage<Order> orders = pipeline
    .readFrom(KafkaSources.kafka(.., "orders"))
    .withoutTimestamps();
StreamStage<OrderDetails> details = orders.mapUsingReplicatedMap("products",
    order -> order.getProductId(),
    (order, product) -> new OrderDetails(order, product));
```

>With a `ReplicatedMap`, a lookup is always local compared to a standard
>`IMap`. The downside is that the data is replicated to all the nodes,
>consuming more memory in the cluster.

## mapUsingService

This transform takes an input and performs a mapping using a _service_
object. Examples are an external HTTP-based service or some library
which is loaded and initialized during runtime (such as a machine
learning model).

The service itself is defined through a `ServiceFactory` object. The
main difference between this operator and a simple `map` is that the
service is initialized once per job. This is what makes it useful for
calling out to heavy-weight objects which are expensive to initialize
(such as HTTP connections).

Let's imagine an HTTP service which returns details for a product and
that we have wrapped this service in a `ProductService` class:

```java
interface ProductService {
    ProductDetails getDetails(int productId);
}
```

We can then create a shared service factory as follows:

```java
StreamStage<Order> orders = pipeline
    .readFrom(KafkaSources.kafka(.., "orders"))
    .withoutTimestamps();
ServiceFactory<?, ProductService> productService = ServiceFactories
    .sharedService(ctx -> new ProductService(url))
    .toNonCooperative();
```

"Shared" means that the service is thread-safe and can be called from
multiple-threads, so only Jet will create just one instance on each
node and share it among the parallel tasklets.

We also declared the service as "non-cooperative" because it makes
blocking HTTP calls. Failing to do this would have severe consequences
for the performance of not just your pipeline, but all the jobs running
on the Jet cluster.

We can then perform a lookup on this service for each incoming order:

```java
StreamStage<OrderDetails> details = orders.mapUsingService(productService,
  (service, order) -> {
      ProductDetails details = service.getDetails(order.getProductId());
      return new OrderDetails(order, details);
  }
);
```

## mapUsingServiceAsync

This transform is identical to [mapUsingService](#mapUsingService) with
one important distinction: the service in this case supports
asynchronous calls, which are compatible with cooperative concurrency
and don't need extra threads. It also means that we can have multiple
requests in flight at the same time to maximize throughput. Instead of
the mapped value, this transform expects the user to supply a
`CompletableFuture<T>` as the return value, which will be completed at
some later time.

For example, if we extend the previous `ProductService` as follows:

```java
interface ProductService {
    ProductDetails getDetails(int productId);
    CompletableFuture<ProductDetails> getDetailsAsync(int productId);
}
```

We still create the shared service factory as before:

```java
StreamStage<Order> orders = pipeline
    .readFrom(KafkaSources.kafka(.., "orders"))
    .withoutTimestamps();
ServiceFactory<?, ProductService> productService = ServiceFactories
    .sharedService(ctx -> new ProductService(url));
```

The lookup instead becomes async, and note that the transform also expects
you to return

```java
StreamStage<OrderDetails> details = orders.mapUsingServiceAsync(productService,
  (service, order) -> {
      CompletableFuture<ProductDetails> f = service
          .getDetailsAsync(order.getProductId);
      return f.thenApply(details -> new OrderDetails(order, details));
  }
);
```

The main advantage of using async communication is that we can have
many invocations to the service in-flight at the same time which will
result in better throughput.

### mapUsingServiceAsyncBatched

This variant is very similar to the previous one, but instead of sending
one request at a time, we can send in so-called "smart batches" (for a
more in-depth look at the internals of Jet, see the [Execution
Engine](../architecture/execution-engine) section). Jet will
automatically group items as they come, and allows to send requests in
batches. This can be very efficient for example for a remote service,
where instead of one roundtrip per request, you can send them in groups
to maximize throughput. If we would extend our `ProductService` as
follows:

```java
interface ProductService {
    ProductDetails getDetails(int productId);
    CompletableFuture<ProductDetails> getDetailsAsync(int productId);
    CompletableFuture<List<ProductDetails>> getAllDetailsAsync(List<Integer> productIds);
}
```

We can then rewrite the transform as:

```java
StreamStage<OrderDetails> details = orders.mapUsingServiceAsyncBatched(productService,
    (service, orderList) -> {
        List<Integer> productIds = orderList
            .stream()
            .map(o -> o.getProductId())
            .collect(Collectors.toList())
        CompletableFuture<List<ProductDetails>> f = service.getDetailsAsync(order.getProductId);
        return f.thenApply(productDetailsList -> {
            List<OrderDetails> orderDetailsList = new ArrayList<>();
            for (int i = 0; i < orderList; i++) {
                new OrderDetails(order.get(i), productDetailsList.get(i)))
          }
      };
  });
);
```

As you can see, there is some more code to write to combine the results
back, but this should give better throughput given the service is able
to efficient batching.

## hashJoin

`hashJoin` is a type of join where you have two or more inputs where all
but one of the inputs must be small enough to fit in memory. You can
consider a _primary_ input which is accompanied by one or more
_side inputs_ which are small enough to fit in memory. The side inputs
are joined to the primary input, which can be either a batch or
streaming stage. The side inputs must be batch stages.

```java
StreamStage<Order> orders = pipeline
    .readFrom(kafka(.., "orders")).withoutTimestamps();
BatchStage<ProductDetails>> productDetails = pipeline
    .readFrom(files("products"));
StreamStage<OrderDetails> joined = orders.hashJoin(productDetails,
        onKeys(order -> order.productId, product -> product.productId),
        (order, product) -> new OrderDetails(order, product)
);
```
