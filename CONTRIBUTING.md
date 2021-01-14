# Contributing to Hazelcast Jet

Thanks for your contribution to Hazelcast Jet.

## Issue Reports

For issue reports, please share with us the following information to
help us resolve your issue quickly and efficiently:

1. Hazelcast Jet version that you use (e.g. 3.2 or 4.0-SNAPSHOT)
2. Logs and stack traces where available.
3. Description of the steps to reproduce your issue or a minimum
   reproducer for the issue, if possible
4. If it's related to integration with an external system (i.e. Kafka)
   details about the external system and configuration parameters for
   connecting to it.

Additional things that may be relevant:

1. Java version. It is also helpful to mention the JVM parameters.
2. Cluster size and configuration
3. Operating system. If it is Linux, kernel version is helpful.

## Pull Requests (PR)

PRs are the main way we accept contributions to Hazelcast Jet.

If you're introducing a new source/sink connector, consider sending the
PR to [hazelcast-jet-contrib](https://github.com/hazelcast/hazelcast-jet-contrib)
repository instead as it acts as an incubator for new Jet features.

* We have a PR review process where each PR is reviewed by the Jet
   development team.
* While your PR is being reviewed, you should sign the [Contributor
   Agreement Form] if this is your first contribution to Hazelcast or
   Hazelcast Jet. This is required to be completed before the PR is
   merged.
* The current development version is in the `master` branch. Patch
   versions are in the `n.n-maintenance` branches.
* Before you create the PR, run the command `mvn clean package -DskipTests`
 in your terminal and fix the CheckStyle and SpotBugs errors (if any).
* Tests will be run on our PR builder and can take very long time to run
  locally, so this is up to you if you want to do this on your own
  environment.
* Please keep your PRs as small as possible, i.e. if you plan to perform
   a huge change, do not submit a single and large PR for it. For an
   enhancement or larger feature, you can create a GitHub issue first to
   discuss.
* If you submit a PR as the solution to a specific issue, please mention
   the issue number either in the PR description or commit message.

[Contributor Agreement
Form]:https://hazelcast.atlassian.net/wiki/display/COM/Hazelcast+Contributor+Agreement
