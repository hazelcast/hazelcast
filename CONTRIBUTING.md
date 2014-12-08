# Contributing to Hazelcast

It makes you feel good...

### Issue Reports
Thanks for reporting your issue.  Please share with us the following information, to help us resolve your issue quickly and efficiently.
*	Hazelcast version that you are using  (e.g. 3.4  also specify minor release or latest snapshot)
2.	Cluster Size (number of Hazelcast member nodes)
3.	Number of Clients
4.	Version of Java (also JVM parameters can be added)
5.	Operating System (for Linux kernel version will be helpful)
6.	Logs and Stack Traces (if available)
7.	Steps to reproduce  (detailed description of steps to reproduce your issue)
8.	Unit Test with hazelcast.xml file (if you can include a unit test which reproduces your issue, we would be grateful)
9.	Integration module versions if available (e.g. tomcat , jetty, spring , hibernate) with Detailed configuration information (web.xml, hibernate configuration, spring context.xml etc.)

### Pull Requests
Thanks for creating pull request.
*	Contributions are submitted, reviewed, and accepted using GitHub pull requests.
2.	In order to merge your pull request, please sing [Contributor Agreement Form].
3.	Try to make clean commits that are easily readable (including descriptive commit messages)
4.	The latest changes are in the master branch.
5.	Run "mvn clean validate -Pcheckstyle" from terminal before you push, if you get a checkstyle error please fix them to can be merge your PR. 
6.	Run "mvn clean install -PfindBugs" from terminal before you push, if you get a findbugs error please fix them to can be merge your PR.
7.	Please keep pull requests as small as possible.
8.	If you are submitting a pull request as a solution to specific issue, please don't forget to mention the issue number either in the pull request description or in commit message.


[Contributor Agreement Form]:https://hazelcast.atlassian.net/wiki/display/COM/Hazelcast+Contributor+Agreement
