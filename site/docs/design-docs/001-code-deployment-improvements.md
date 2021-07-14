---
title: 001 - Code Deployment Improvements
description: Improvements for adding classes during job submission
---

*Since*: 4.1

## Background

User should be able to add nested (inner & anonymous) classes as well as
entire packages to Jet job’s classpath.

## Problem statement

In Java there is no reflective way to fetch anonymous classes used in a
given class. Moreover, ClassLoaders don’t support listing package
content.

## Solution

As nested classes are created under the same package as the root class,
one can list all the resources associated with given package in the form
of URLs and then extract & filter wanted class files - listing files
under a given directory, extracting files form arbitrary nested jar
files, handling custom URL schemes etc. Similar strategy could be
employed to recursively find all classes in a given package.

There is already quite lightweight (470KB), open source library -
[classgraph](https://github.com/classgraph/classgraph) -  supporting
wide range of classpath specification mechanisms with the help of which
one is able to find all the resources belonging to the given package. To
make use of it Jet shades above mentioned jar.

## API changes

Extend existing `JobConfig.addClass()` so it not only adds the given
classes but also all their nested classes to the Jet job’s classpath:

```java
/**
 * Adds the given classes and recursively all their nested classes to the
 * Jet job's classpath. They will be accessible to all the code attached
 * to the underlying pipeline or DAG, but not to any other code.
 * (An important example is the {@code IMap} data source, which can
 * instantiate only the classes from the Jet instance's classpath.)
 * <p>
 * See also {@link #addJar} and {@link #addClasspathResource}.
 *
 * @implNote Backing storage for this method is an {@link IMap} with a
 * default backup count of 1. When adding big files as a resource, size
 * the cluster accordingly in terms of memory, since each file will have 2
 * copies inside the cluster(primary + backup replica).
 *
 * @return {@code this} instance for fluent API
 * @since 4.1
 */
@Nonnull
@SuppressWarnings("rawtypes")
public JobConfig addClass(@Nonnull Class... classes)
```

Add `JobConfig.addPackage()` that adds recursively all the classes and
resources in given packages to the Jet job's classpath

```java
/**
 * Adds recursively all the classes and resources in given packages
 * to the Jet job's classpath. They will be accessible to all the
 * code attached to the underlying pipeline or DAG, but not to any
 * other code. (An important example is the {@code IMap} data source,
 * which can instantiate only the classes from the Jet instance's
 * classpath.)
 * <p>
 * See also {@link #addJar} and {@link #addClasspathResource}.
 *
 * @implNote Backing storage for this method is an {@link IMap} with a
 * default backup count of 1. When adding big files as a resource, size
 * the cluster accordingly in terms of memory, since each file will hav
 * copies inside the cluster(primary + backup replica).
 *
 * @return {@code this} instance for fluent API
 */
@Nonnull
public JobConfig addPackage(@Nonnull String... packages)
```

## Tests

In particular created unit test proving Kotlin lambdas (as anonymous
classes) are added to the classpath.

## Improvements

Maybe instead of ‘forcing' user to manually add (`addClass()`,
`addPackage()`, `addJar()` …) all the required resources to the
classpath we could scan the classpath and add them automatically -
however we would need to figure out the way to filter out unneeded ones
so we don’t end up with bloated job’s `IMap` state.
