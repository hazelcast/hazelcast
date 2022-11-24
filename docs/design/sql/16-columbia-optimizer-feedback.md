# COLUMBIA OPTIMIZER INTEGRATION NOTES & FEEDBACK

| ℹ️ Since: 5.x |
|---------------|

|||
|---|---|
|Related Github issues|[Pull Request](https://github.com/hazelcast/hazelcast/pull/22480)|
|Document Status / Completeness|REJECTED|
|Developer(s)|Sasha Syrotenko|
|Technical Reviewers|Viliam Durina|

## Description

This document describes design, efforts and feature rejection reasons 
of transferring Hazelcast SQL engine to Columbia optimizer.

## Design

Apache Calcite introduced Columbia optimizer implementation with 1.24 release. 
It implements CASCADES optimization algorithm designed by Columbia optimizer creator,
described [here](https://15721.courses.cs.cmu.edu/spring2019/papers/22-optimizer1/xu-columbia-thesis1998.pdf).

Apache Calcite names its Columbia optimizer implementation as 
a "[Top-Down Rule Driver](https://issues.apache.org/jira/browse/CALCITE-3916)". We will also name it with that style : Top-Down Optimizer.

To enable the top-down rule driver, the user should enable `calcite.planner.topdown.opt` option.

### Usage Benefits

Main benefit to replace Volcano optimizer with new Columbia optimizer is to reduce optimization search space for operators 
like `Index Scan` or `Merge Join`.

### Top-Down Optimizer User Perspective

Main requirement to achieve CASCADES-style optimization is to implement `PhysicalNode` interface for each _physical_ rel available for the optimizer. 

**(!)** We need to note that the meaning of Hazelcast _physical_ relation and Top-Down Optimizer's _physical_ node are different.

It contains 4 methods to implement : 
- `passThrough` -- propagates the trait request from given rel to its children rels. In most common case it returns a copy of original rel with satisfied propagated traits. 
But, in some cases original rel may be replaced with another rel based on propagated trait analysis.
For example, it may impose `Full Scan -> Index Scan` conversion based on collation trait state and sorted index for collated attribute availability. 
In our prototype, the majority of relations uses default implementation, and only a few rels like `FullScanPhysicalRel` defines custom logic.

- `passThroughTraits` defines trait propagation logic for given rel. 
Some nodes just forward traits, but the majority of rels are using their custom trait propagation logic.

- `derive` - bubbles traits up for given rel. Good use case of trait derivation is described [here, pt.4](https://lists.apache.org/thread/fn1wwkb62byk2vlpqqsgmsllj6xjgprq).

- `deriveTraits` - defines traits derivation logic. For example:  when sorted `Index Scan` is picked and 
that collation was requested by `Sort` rel, it may be just eliminated due to redundancy.

### Optimizer Application Issues

As it was mentioned above, the meaning of Hazelcast _physical_ relation and Top-Down Optimizer's _physical_ node are different.
Hazelcast has 3 optimization phases (state of 5.2.0) : 
- unconditional rel tree rewrites, like `Project -> Calc` or `Filter -> Calc` conversions.
- logical phase : default Calcite rel tree with `NONE` convention rewrites with `LOGICAL` convention 
with a few rel transpositions or merges.  
- physical phase : logical Calcite rel tree with `LOGICAL` convention rewrites with `PHYSICAL` convention and applies
a lot of implementation-specific optimization rules.

The issue here is that Top-Down Optimizer **apply the rules immediately for the first convention conversion, 
what is `NONE -> LOGICAL` in our case.**

We tried to avoid that limitation approaching these actions:
1. Use HEP planner on logical opt phase - didn't work for us, HEP planner applies rules exhaustively in strict order, 
and there is no one correct order when all rules would work correctly.
To know more, [see prototype PR](https://github.com/hazelcast/hazelcast/pull/22559). 
2. Use separate Volcano planner for logical and physical phases - that just doesn't work in Calcite. 
All optimizations should be done by only planner, it's implementation specifics of Calcite.
3. Get rid of logical phase, move all mandatory rules to physical phase - that requires big amount of efforts and high risk. 
That factors don't give any payback from feature value POV, so, that option also was discarded.

Based on that, SQL team decided to postpone all research in optimizer upgrade direction.

### Future Changes

The transfer to Top-Down Optimizer was partially implemented in pull request mentioned in header of the TDD. 
This code is ready to be applied in future efforts.