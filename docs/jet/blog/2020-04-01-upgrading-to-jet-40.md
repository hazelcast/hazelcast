---
title: Upgrading to Jet 4.0
author: Bartók József
authorURL: https://www.linkedin.com/in/bjozsef/
authorImageURL: https://www.itdays.ro/public/images/speakers-big/Jozsef_Bartok.jpg
---

As we have announce earlier [Jet 4.0 is out](/blog/2020/03/02/jet-40-is-released)!
In this blog post we aim to give you the lower level details needed for
migrating from older versions.

Jet 4.0 is a major version release. According to the semantic versioning
we apply, this means that in version 4.0 some of the API has changed in
a breaking way and code written for 3.x may no longer compile against
it.

## Jet on IMDG 4.0

Jet 4.0 uses IMDG 4.0, which is also a major release with its own
breaking changes. For details see [IMDG Release Notes](https://docs.hazelcast.org/docs/rn/index.html#4-0)
and [IMDG Migration Guides](https://docs.hazelcast.org/docs/4.0/manual/html-single/#migration-guides).

The most important changes we made and which have affected Jet too are
as follows:

* We renamed many packages and moved classes around. For details see the
  [IMDG Release Notes](https://docs.hazelcast.org/docs/rn/index.html#4-0).
  The most obvious change is that many classes that used to be in the
  general `com.hazelcast.core` package are now in specific packages like
  `com.hazelcast.map` or `com.hazelcast.collection`.

* `com.hazelcast.jet.function`, the package containing serializable
  variants of `java.util.function`, is now merged into
  `com.hazelcast.function`: `BiConsumerEx`, `BiFunctionEx`,
  `BinaryOperatorEx`, `BiPredicateEx`, `ComparatorEx`, `ComparatorsEx`,
  `ConsumerEx`, `FunctionEx`, `Functions`, `PredicateEx`, `SupplierEx`,
  `ToDoubleFunctionEx`, `ToIntFunctionEx`, `ToLongFunctionEx`.

* `EntryProcessor` and several other classes and methods received a
  cleanup of their type parameters. See the [relevant section](https://docs.hazelcast.org/docs/4.0/manual/html-single/#introducing-lambda-friendly-interfaces)
  in the IMDG Migration Guide.

* The term "group" in configuration was replaced with "cluster". See the
  code snippet below for an example. This changes a Jet Command Line
  parameter as well (`-g/--groupName` renamed to `-n/--cluster-name`).

  ```java
  clientConfig.setClusterName("cluster_name");
  //clientConfig.getGroupConfig().setName("cluster_name")
  ```

* `EventJournalConfig` moved from the top-level Config class to data
  structure-specific configs (`MapConfig`, `CacheConfig`):

  ```java
  config.getMapConfig("map_name").getEventJournalConfig();
  //config.getMapEventJournalConfig("map_name")
  ```

* `ICompletableFuture` was removed and replaced with the JDK-standard
  `CompletionStage`. This affects the return type of async methods. See
  the [relevant section](https://docs.hazelcast.org/docs/4.0/manual/html-single/#removal-of-icompletablefuture)
  in the IMDG Migration Guide.

## Jet API Changes

We made multiple breaking changes in Jet’s own APIs too:

* `IMapJet`, `ICacheJet` and `IListJet`, which used to be Jet-specific
  wrappers around IMDG’s standard `IMap`, `ICache` and `IList`, were
  removed. The methods that used to return these types now return the
  standard ones.

* Renamed `Pipeline.drawFrom` to `Pipeline.readFrom` and
  `GeneralStage.drainTo` to `GeneralStage.writeTo`:

  ```java
  pipeline.readFrom(TestSources.items(1, 2, 3)).writeTo(Sinks.logger());
  //pipeline.drawFrom(TestSources.items(1, 2, 3)).drainTo(Sinks.logger());
  ```

* `ContextFactory` was renamed to `ServiceFactory` and we added support
  for instance-wide initialization. createFn now takes
  `ProcessorSupplier.Context` instead of just `JetInstance`. We also
  added convenience methods in `ServiceFactories` to simplify
  constructing the common variants:

  ```java
  ServiceFactories.sharedService(ctx -> Executors.newFixedThreadPool(8), ExecutorService::shutdown);
  //ContextFactory.withCreateFn(jet -> Executors.newFixedThreadPool(8)).withLocalSharing();

  ServiceFactories.nonSharedService(ctx -> DateTimeFormatter.ofPattern("HH:mm:ss.SSS"), ConsumerEx.noop());
  //ContextFactory.withCreateFn(jet -> DateTimeFormatter.ofPattern("HH:mm:ss.SSS"))
  ```

* `map/filter/flatMapUsingContext` was renamed to
  `map/filter/flatMapUsingService`:

  ```java
  pipeline.readFrom(TestSources.items(1, 2, 3))
          .filterUsingService(
                  ServiceFactories.sharedService(pctx -> 1),
                  (svc, i) -> i % 2 == svc)
          .writeTo(Sinks.logger());

  /*
  pipeline.drawFrom(TestSources.items(1, 2, 3))
          .filterUsingContext(
                  ContextFactory.withCreateFn(i -> 1),
                  (ctx, i) -> i % 2 == ctx)
          .drainTo(Sinks.logger());
  */
  ```

* `filterUsingServiceAsync` has been removed. Usages can be replaced
  with `mapUsingServiceAsync`, which behaves like a filter if it returns
  a `null` future or the returned future contains a `null` result:

  ```java
  stage.mapUsingServiceAsync(serviceFactory,
          (executor, item) -> {
              CompletableFuture<Long> f = new CompletableFuture<>();
              executor.submit(() -> f.complete(item % 2 == 0 ? item : null));
              return f;
          });
  /*
  stage.filterUsingServiceAsync(serviceFactory,
          (executor, item) -> {
              CompletableFuture<Boolean> f = new CompletableFuture<>();
              executor.submit(() -> f.complete(item % 2 == 0));
              return f;
          });
  */
  ```

* `flatMapUsingServiceAsync` has been removed. Usages can be replaced
  with `mapUsingServiceAsync` followed by non-async `flatMap`:

  ```java
  stage.mapUsingServiceAsync(serviceFactory,
          (executor, item) -> {
              CompletableFuture<List<String>> f = new CompletableFuture<>();
              executor.submit(() -> f.complete(Arrays.asList(item + "-1", item + "-2", item + "-3")));
              return f;
          })
          .flatMap(Traversers::traverseIterable);
  /*
  stage.flatMapUsingServiceAsync(serviceFactory,
          (executor, item) -> {
              CompletableFuture<Traverser<String>> f = new CompletableFuture<>();
              executor.submit(() -> f.complete(traverseItems(item + "-1", item + "-2", item + "-3")));
              return f;
          })
  */
  ```

* The methods `withMaxPendingCallsPerProcessor(int)` and
  `withUnorderedAsyncResponses()` were removed from `ServiceFactory`.
  These properties are relevant only in the context of asynchronous
  operations and were used in conjunction with
  `GeneralStage.mapUsingServiceAsync(…)`. In Jet 4.0 the
  `GeneralStage.mapUsingServiceAsync(…)` method has a new variant with
  explicit parameters for the above settings:

  ```java
  stage.mapUsingServiceAsync(
          ServiceFactories.sharedService(ctx -> Executors.newFixedThreadPool(8)),
          2,
          false,
          (exec, task) -> CompletableFuture.supplyAsync(() -> task, exec)
  );

  /*
  stage.mapUsingContextAsync(
          ContextFactory.withCreateFn(jet -> Executors.newFixedThreadPool(8))
                  .withMaxPendingCallsPerProcessor(2)
                  .withUnorderedAsyncResponses(),
          (exec, task) -> CompletableFuture.supplyAsync(() -> task, exec)
  );
  */
  ```

* `com.hazelcast.jet.pipeline.Sinks#mapWithEntryProcessor` got a new
  signature in order to accommodate the improved `EntryProcessor`, which
  became more lambda-friendly in IMDG (see the [relevant section](https://docs.hazelcast.org/docs/4.0/manual/html-single/#introducing-lambda-friendly-interfaces)
  in the IMDG Migration Guide). The return type of `EntryProcessor` is
  now an explicit parameter in ``mapWithEntryProcessor``'s method
  signature:

  ```java
  FunctionEx<Map.Entry<String, Integer>, EntryProcessor<String, Integer, Void>> entryProcFn =
          entry ->
                  (EntryProcessor<String, Integer, Void>) e -> {
                      e.setValue(e.getValue() == null ? 1 : e.getValue() + 1);
                      return null;
                  };
  Sinks.mapWithEntryProcessor(map, Map.Entry::getKey, entryProcFn);

  /*
  FunctionEx<Map.Entry<String, Integer>, EntryProcessor<String, Integer>> entryProcFn =
          entry ->
                  (EntryProcessor<String, Integer>) e -> {
                      e.setValue(e.getValue() == null ? 1 : e.getValue() + 1);
                      return null;
                  };
  Sinks.mapWithEntryProcessor(map, Map.Entry::getKey, entryProcFn);
  */
  ```

* HDFS source and sink methods are now `Hadoop.inputFormat` and
  `Hadoop.outputFormat`.

* `MetricsConfig` is no longer part of `JetConfig`, but resides in the
  IMDG `Config` class:

  ```java
  jetConfig.getHazelcastConfig().getMetricsConfig().setCollectionFrequencySeconds(1);
  //jetConfig.getMetricsConfig().setCollectionIntervalSeconds(1);
  ```

* `Traverser` type got a slight change in the `flatMap` lambda’s generic
  type wildcards. This change shouldn’t affect anything in practice.

* In sources and sinks we changed the method signatures so that the
  lambda becomes the last parameter, where applicable.

* `JetBootstrap.getInstance()` moved to `Jet.bootstrappedInstance()` and
  now it automatically creates an isolated local instance when not
  running through `jet submit`. If used from `jet submit`, the behaviour
  remains the same.

* `JobConfig.addResource(…)` is now `addClasspathResource(…)`.

* `ResourceType`, `ResourceConfig` and `JobConfig.getResourceConfigs()`
  are now labeled as private API and we discourage their direct usage.
  We also renamed `ResourceType.REGULAR_FILE` to `ResourceType.FILE`,
  but this is now an internal change.

## Further help

In case you encounter any difficulties with migrating to Jet 4.0 feel
free to [contact us any time](https://gitter.im/hazelcast/hazelcast-jet).
