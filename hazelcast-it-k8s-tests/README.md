#### Kubernetes based integration testing

The code was structured in two modules:

- hazelcast-it-k8s - Support for generic Kubernetes-based integration tests
- hazelcast-it-k8s-tests - Hazelcast Integration Tests For Kubernetes and Hazelcast specific support

### Prerequisites

- A running Kubernetes cluster (verify with `kubectl cluster-info`)

### Creating tests

There are few main components, the most important are:

- `KubernetesTestHelper` - provides main methods for interacting with the cluster
- `HazelcastHelmTestHelper`- provides Hazelcast specific methods, e.g. installing Hazelcast helm

A typical workflow could look like the one below:

- (optionally) create a custom namespace (to avoid conflicts between parallel tests)

```java
kubernetesTestHelper.createOrReplaceNamespace("my-custom-namespace");
```

- Install Hazelcast cluster on Kubernetes cluster
```java
hazelcastHelmTestHelper.installHazelcastUsingHelmChart("my-release-name", HZ_MEMBER_COUNT)
```
- Perform specific test calls using helper or fallbacking to the low level Fabric8 Kubernetes client
```java
String log = testHelper.kubernetesClient().pods().withName("my-release-name-hazelcast-0").getLog();
assertThat(log).contains(":5701 is STARTED");
```

See also Fabric8 Kubernetes client docs for more examples:
- https://github.com/fabric8io/kubernetes-client/blob/master/doc/CHEATSHEET.md
- https://github.com/fabric8io/kubernetes-client#kubectl-java-equivalents

Take a look at `SampleHelmTest`as an example of the test.

### Setting up a GCP Kubernetes cluster

- Setup GCP locally:
```shell
gcloud init
```
- Create a Kubernetes cluster
```shell
gcloud container clusters create <my-cluster>
```
- Get credentials for you GCP Kubernetes cluster:
```shell
gcloud container clusters get-credentials <my-cluster>
```

### Using custom docker image with a local codebase using GCP

- Setup authentication for the artifact registry on GCP
```shell
gcloud auth configure-docker us-east1-docker.pkg.dev
```

- Create docker registry
Needed for storing custom docker images, can be shared across the teams
```shell
gcloud artifacts repositories create <my-docker-repository> --location us-east1 --repository-format=docker
```

- Build and push docker image to GCP

You can use your custom codebase for tests by building local docker image and pushing it to the GCP Artifact Registry.
In order to do that:
- Set DOCKER_REPO variable:
```shell
export DOCKER_REPO="us-east1-docker.pkg.dev/<project>/<repository-name>"
```
Run the script:
```shell
hazelcast-it-k8s-tests/build-and-push-local-docker-image.sh
```

When the image is pushed note down its location, something like:
```
 us-east1-docker.pkg.dev/<project>/<repository-name>/hazelcast:<your-name>-k8s-testing
```
and use it in your tests, see `SampleCustomCodebaseHelmTest` for a reference.

Remember to remove your custom image afterwards:
```shell
gcloud artifacts docker tags delete us-east1-docker.pkg.dev/<project>/<repository-name>/hazelcast:<your-name>-k8s-testing
```
and cluster:
```shell
gcloud container clusters delete <my-cluster>
```
### Possible improvements

- Add support for non-default Kubernetes contexts
