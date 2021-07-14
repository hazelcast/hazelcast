# Contributing to Hazelcast

Hazelcast is Open Source software, licensed under the [Apache 2.0. license](LICENSE).
The main benefit of Open Source is that you don't need to wait for a vendor to provide a fix or a feature.
If you've got the skills (and the will), it's already at your fingertips.

There are multiple ways to contribute:

1. [Reporting an issue](#issue-reports)
2. [Sending a pull request](#pull-requests).
Note that you don't need to be a developer to help us.
Contributions that improve the documentation are always appreciated.

If you feel yourself in need of assistance, please reach us directly via [Slack](https://slack.hazelcast.com/).

## Issue Reports

Thanks for reporting your issue.
To help us resolve your issue quickly and efficiently, we need as much data for diagnostics as possible.
Please share with us the following information:

1.	Exact Hazelcast version that you use (_e.g._ `4.0.1`, also whether it is a minor release, or the latest snapshot).
2.	Cluster size, _i.e._ the number of Hazelcast cluster members.
3.	Number of clients.
4.	Java version.
It is also helpful to mention the JVM parameters.
5.	Operating system.
If it is Linux, kernel version is helpful.
6.	Logs and stack traces, if available.
7.	Detailed description of the steps to reproduce your issue.
8.	Unit test with the `hazelcast.xml` file.
If you could include a unit test which reproduces your issue, it would speed the resolution time.
9.	If it's related to integration with an external system
      (_e.g._ Tomcat, Spring, Hibernate, Kafka) details about the external
      system and configuration parameters for connecting to it.

## Pull requests

Thanks a lot for creating your <abbr title="Pull Request">PR</abbr>!

A PR can target many different subjects:

* [Documentation](https://github.com/hazelcast/hazelcast-reference-manual):
either fix typos or improve the documentation as a whole
* Fix a bug
* Add a feature
* Add additional tests to improve the test coverage, or fix flaky tests
* Anything else that makes Hazelcast better!

All PRs follow the same process:

1.	Contributions are submitted, reviewed, and accepted using the PR system on GitHub.
2.	For first time contributors, our bot will automatically ask you to sign the Hazelcast Contributor Agreement on the PR.
3.	The latest changes are in the `master` branch.
4.	Make sure to design clean commits that are easily readable.
That includes descriptive commit messages.
5.	Please keep your PRs as small as possible, _i.e._ if you plan to perform a huge change, do not submit a single and large PR for it.
For an enhancement or larger feature, you can create a GitHub issue first to discuss.
6.	Before you push, run the command `mvn clean validate` in your terminal and fix the CheckStyle errors if any.
Push your PR once it is free of CheckStyle errors.
7.	If you submit a PR as the solution to a specific issue, please mention the issue number either in the PR description or commit message.
