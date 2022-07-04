---
title: 003 - Elasticsearch Connector
description: Elasticsearch Connector (source and sink)
---

*Since*: 4.2

## Background

Existing Elasticsearch connector doesn't support all features expected
of production ready connector. It is not well covered with automated
tests.

## Implementation

### Choice of Client

Elasticsearch provides two Java clients:

#### Java Low Level REST Client

- has minimal dependencies
- must parse all json responses ourselves (and update during upgrades)
- [Low level client](https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-low.html)

#### Java High Level REST Client

- This client usually used by elasticsearch users from Java
- [High level client](https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high.html)

Pros:

- Provides users with API they already know and use, e.g.

```java
p.readFrom(ElasticSources.elasticsearch("users", () -> createClient(containerAddress),
  () -> {
      SearchRequest searchRequest = new SearchRequest("users");
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.query(termQuery("age", 8));
      searchRequest.source(searchSourceBuilder);
      return searchRequest;
})).writeTo(Sinks.list("sink"));
```

- Reduces our maintenance - the client is published with new
  elasticsearch versions, any updates to the REST api are included in
  the new client. If there are changes to the client’s java API then
  these will be likely easier than updating custom implementation.

Cons:

- The client has 40 MB of dependencies.
- Doesn't actually support all APIs we need - the shard api is
  missing, but this can be implemented using the low level client
  the following way:

```java
Request r = new Request("GET", "/_cat/shards/" + String.join(",", sr.indices()));
r.addParameter("format", "json");
Response res = client.getLowLevelClient().performRequest(r);
try (InputStreamReader reader =
        new InputStreamReader(res.getEntity().getContent())) {
    JsonArray array = Json.parse(reader).asArray();
    ....
```

Any custom client implementation would use Elastic REST API, same as the
high level rest client and would likely be very similar in terms of API,
so swapping out later for custom implementation should not be difficult.

Based on these arguments we chose to use the **High Level REST Client**

### Factory method vs builder

This is the current API for all connector settings:

```java
public static <T> BatchSource<T> elasticsearch(
      @Nonnull String name,
      @Nonnull SupplierEx<? extends RestHighLevelClient> clientSupplier,
      @Nonnull SupplierEx<SearchRequest> searchRequestSupplier,
      @Nonnull String scrollTimeout,
      @Nonnull FunctionEx<SearchHit, T> mapHitFn,
      @Nonnull FunctionEx<? super ActionRequest, RequestOptions> optionsFn,
      @Nonnull ConsumerEx<? super RestHighLevelClient> destroyFn
) {
```

New requirements introduce additional settings (slicing, co-located
reading, ..) which would make the list of parameters too long. A builder
class is implemented for both the source and sink to provide same
experience.

Full example:

```java
BatchSource<String> elasticSource = new ElasticSourceBuilder<String>()
  .name("my-elastic-source")
  .clientFn(elasticClientSupplier())
  .searchRequestFn(() -> new SearchRequest("my-index-*"))
  .optionsFn(request -> RequestOptions.DEFAULT)
  .mapToItemFn(SearchHit::getSourceAsString)
  .slicing(true)
  .build();
```

Minimal example:

```java
BatchSource<String> elasticSource = new ElasticSourceBuilder<String>()
  .clientFn(() -> client("elastic", "password", "localhost", 9200))
  .searchRequestFn(SearchRequest::new)
  .mapToItemFn(SearchHit::getSourceAsString)
  .build();
```

### SearchRequest vs SupplierEx<SearchRequest>

It was suggested during the review that we could leverage Elastic's Writable
to serialize the search request to avoid the use of a supplier. It wasn't
implemented in the end for following reasons:

- the serialization/deserialization is different across Elastic
 versions, making it harder to maintain
- the deserialization requires non-trivial setup, consisting of
 internals of Elastic transport (communication between Elastic nodes) classes.

## New features

### Slicing

Slicing is used to parallelize read from Elasticsearch. See: [Sliced scroll](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html#sliced-scroll)

To provide maximum performance the number of slices should be less than
number of shards.

Each processor reads one or more shards. If there are not enough shards
then some processors don’t read any data.

It is possible to create more slices than shards, but it has high
initial latency and consumes more memory on Elasticsearch side. See
linked documentation.

### Co-located read/write

In deployment scenario where Jet Cluster and Elasticsearch cluster run
on the same set of nodes it is beneficial to read from local
Elasticsearch node.

This is done by setting nodes of low level client to local node only

```java
client.getLowLevelClient().setNodes(...);
```

and setting preference on the search request

```java
sr.preference("_shards:0|_only_local");
```

This also limits reading to shard 0 only, which is needed to ensure
single shard replica is read by one processor.

### Assignment of shards

Shard numbers are not unique, shard is identified by index name and
shard number. Shard has a primary and 0 or more replicas.

`ElasticProcessorMetaSupplier` reads all available shards for given search
request `/_cat/shards/indexes-from-search-request*`. Shards located on
each node become candidates. A shard from list of candidates is assigned
to a node, iterating over all nodes. Assigned shard is removed from list
of candidates. Assignment is finished when all shards are assigned (to
exactly 1 node).

## Authentication

Because we use the High Level REST client users use the same
authentication methods as they normally would.
A convenience factory method for authenticated client for basic
authentication is provided:

```java
public static RestHighLevelClient client(
  @Nonnull String username,
  @Nullable String password,
  @Nonnull String hostname,
  int port
)
```

## Testing

Code which can be tested in isolation is covered by unit tests (e.g.
partition assignment).

Most tests are actually integration tests. Testcontainers library is
used for integration testing to run Elasticsearch.

Following test hierarchy is used:

- abstract `BaseElasticsearchTest` - base class for all tests of Jet
  and Elasticsearch together,
  no actual tests, only setup / teardown code
- `CommonElasticSourcesTest` - tests that are to be executed on
  all environment configurations
- subclasses of `CommonElasticSourcesTest` which define specific
  environment (single Jet instance, Jet cluster ..)
