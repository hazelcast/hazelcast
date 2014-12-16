# Contributing to Hazelcast

It makes you feel good...

## Issue Reports
Thanks for reporting your issue.  Please share with us the following information, to help us resolve your issue quickly and efficiently.
*	Hazelcast version that you use (e.g. 3.4, also specify whether it is a minor release or the latest snapshot).
2.	Cluster size, i.e. the number of Hazelcast cluster members.
3.	Number of the clients.
4.	Version of Java. It is also helpful to mention the JVM parameters.
5.	Operating system. If it is Linux, kernel version is helpful.
6.	Logs and stack traces, if available.
7.	Detailed description of the steps to reproduce your issue.
8.	Unit test with the `hazelcast.xml` file. If you could include a unit test which reproduces your issue, we would be grateful.
9.	If available, integration module versions (e.g. Tomcat, Jetty, Spring, Hibernate). Also, include their  detailed configuration information such as `web.xml`, Hibernate configuration and `context.xml` for Spring.

## Pull Requests
Thanks for creating your pull request (PR).
*	Contributions are submitted, reviewed, and accepted using the pull requests on GitHub.
2.	In order to merge your PR, please sign the [Contributor Agreement Form].
3.	Try to make clean commits that are easily readable (including descriptive commit messages).
4.	The latest changes are in the **master** branch.
5.	Before your push, run the command `mvn clean package -P findBugs,checkstyle` at your terminal and fix the checkstyle and findbugs errors (if any). Push your PR that is free of checkstyle and findbugs errors.
6.	Please keep your PRs as small as possible, i.e. if you plan to perform a huge change, do not submit a single and large PR for it. For an enhancement or larger feature,you can create a GitHub issue first to discuss.
7.	If you submit a PR as the solution to a specific issue, please mention the issue number either in the PR description or commit message.


[Contributor Agreement Form]:https://hazelcast.atlassian.net/wiki/display/COM/Hazelcast+Contributor+Agreement
