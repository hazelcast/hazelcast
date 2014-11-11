
# Hazelcast JCache

This chapter describes the basics of JCache: the standardized Java caching layer API. The JCache
caching API is specified by the Java Community Process (JCP) as Java Specification Request (JSR) 107.

Caching keeps data in memory that either are slow to calculate/process or originate from another underlying backend system
whereas caching is used to prevent additional request round trips for frequently used data. In both cases, caching could be used to
gain performance or decrease application latencies.

## JCache Overview

Starting with Hazelcast release 3.3.1, a specification compliant JCache implementation is offered. To show our commitment to this
important specification the Java world was waiting for over a decade, we do not just provide a simple wrapper around our existing
APIs but implemented a caching structure from ground up to optimize the behavior to the needs of JCache. As mentioned before,
the Hazelcast JCache implementation is 100% TCK (Technology Compatibility Kit) compliant and therefore passes all specification
requirements.

In addition to the given specification, we added some features like asynchronous versions of almost all
operations to give the user extra power.  

This chapter gives a basic understanding of how to configure your application and how to setup Hazelcast to be your JCache
provider. It also shows examples of basic JCache usage as well as the additionally offered features that are not part of JSR-107.
To gain a full understanding of the JCache functionality and provided guarantees of different operations, read
the specification document (which is also the main documentation for functionality) at the specification page of JSR-107:

[https://www.jcp.org/en/jsr/detail?id=107](https://www.jcp.org/en/jsr/detail?id=107)

