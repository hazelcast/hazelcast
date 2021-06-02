# Merge Changes Summary

The Hazelcast and Hazelcast Jet products were merged together to provide a
seamless experience. Changes done during the merge of Hazelcast and Hazelcast
Jet codebases are summarized in this document.

Hazelcast Jet repository was merged into Hazelcast repository.

The name Jet doesn't refer to standalone product anymore, but is kept when
referring to the streaming engine part of the product - the Jet engine.

Hazelcast repository - https://github.com/hazelcast/hazelcast
Hazelcast Jet repository - https://github.com/hazelcast/hazelcast-jet

After some preparatory commits removing conflicting files (e.g. root pom.xml),
the merge was performed by the following command:

```bash
$ git merge --allow-unrelated-histories hazelcast-jet/master
``` 

This preserves history for both projects and allows easy rebasing of patches
both ways (forward porting and backporting from/to Hazelcast Jet).

Hazelcast repository continues its development as with a next major version.

## Module changes

- `hazelcast-jet-core` code was merged into `hazelcast` module
- `hazelcast-jet-spring` module was merged into `hazelcast-spring` and removed
- `hazelcast-jet-distribution` module was removed, `hazelcast-distribution`
  module was adapted to produce very similar artifacts - full and slim
  distribution
- the extension modules were kept under `extensions/*`, keeping the groupId and
  artifactId coordinates, this makes it easy for existing users to migrate.
- `examples` modules were deleted, they will be merged with imdg example, which
  are stored in a separate repository at https://github.com/hazelcast/hazelcast-code-samples/
- `hazelcast-jet-sql` module was merged into `hazelcast-sql` module (this
  actually happened after the merge, but for users it is indistinguishable).`

## Code Changes

- JetNodeExtension was merged with DefaultNodeExtension
- The Jet service is started when a HazelcastInstance is started
- HazelcastInstance can return JetInstance via #getJetInstance (this will
  change before the release into a `JetService getJet()` where `JetService`
  provides a subset of `JetInstance` methods)
- Jet datastructures created by com.hazelcast.jet.impl.JobRepository are
  created lazily when needed
- JetConfig is now a field in `com.hazelcast.config.Config`
- Jet run tests in parallel by default, Jet tests were marked with `QuickTest`
  and `ParallelJVMTest` `@Category` accordingly
- IMDG's `smallInstanceConfig()` for tests sets the
  `com.hazelcast.jet.config.InstanceConfig#setCooperativeThreadCount` to 2

## Checkstyle

- Jet code was adapted to IMDG checkstyle rules
- some checkstyle rules were relaxed

There are 2 outstanding items to resolve:
- some checkstyle rules were ignored for Jet code
- Jet had some stricker rules regarding public javadoc, this is now not in
  place, ideally we should bring whole  

