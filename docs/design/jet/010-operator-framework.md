---
title: 010 - Kubernetes Operators
description: Helm based operator implementation for Hazelcast Jet.
---

*Since*: 4.2

## Summary

Kubernetes is a portable, extensible, open-source platform for managing
containerized workloads and services, that facilitates both declarative
configuration and automation. It has a large, rapidly growing ecosystem.
Kubernetes services, support, and tools are widely available.

An Operator extends Kubernetes to automate the management of the entire
lifecycle of a particular application. Operators serve as a packaging
mechanism for distributing applications on Kubernetes, and they monitor,
maintain, recover, and upgrade the software they deploy.

## Terminology

**Operator** - the custom controller installed on a Kubernetes cluster,
in our case Hazelcast Jet Operator.

**Operand** - the managed workload provided by the Operator as a
service, in our case Hazelcast Jet cluster.

**Custom Resource (CR)** - an instance of the
[`CustomResourceDefinition`](https://kubernetes.io/docs/tasks/access-kubernetes-api/custom-resources/custom-resource-definitions/)
the Operator ships that represents the Operand or an Operation on an
Operand (also known as primary resources)

**Managed resources** - the Kubernetes objects or off-cluster services
the Operator uses to constitute an Operand (also known as secondary
resources)

**Custom Resource Definition (CRD)** - an API of the Operator, providing
the blueprint and validation rules for **Custom Resources**.

## Implementation Details

### Operator SDK

SDK for building Kubernetes applications. Provides high level APIs,
useful abstractions, and project scaffolding.

The SDK provides workflows to develop operators in Go, Ansible, or Helm.

Since we've already had Helm packages for Hazelcast Jet and Hazelcast
Jet Enterprise, it makes sense to create first version of the operator
based on Helm.

Operator SDK provides a CLI tool to generate Operators from Helm charts,
however it is not sufficient to use to generated Operator as is, some
tweaks are needed to fine tune its permissions.

### Operator Capability Levels

Operators come in different maturity levels in regards to their
lifecycle management capabilities for the application or workload they
deliver. The capability models aims to provide guidance in terminology
to express what features users can expect from an operator.

![Operator Capability Levels](/docs/assets/operator-capability-level.png)

Hazelcast Jet Helm based operators currently categorized as Level 1
operator.

### Operator Lifecycle Manager (OLM)

OLM extends Kubernetes to provide a declarative way to install, manage,
and upgrade Operators and their dependencies in a cluster.

OLM defines a schema for Operator metadata, called the Cluster Service
Version (CSV), for describing an Operator and its dependencies.

Operators with a CSV can be listed as entries in a catalog (i.e
[OperatorHub.io](https://operatorhub.io/)) available to OLM running on a
Kubernetes cluster. Users then subscribe to an Operator from the catalog
to tell OLM to provision and manage a desired Operator. That Operator,
in turn, provisions and manages its application or service on the
cluster.

Operator SDK CLI tool provides capability to generate CSVs but it's far
from catalog listing criteria so manual curation of CSVs needed in our
case. After creating the initial version of the CSV, making updates to
it is pretty straightforward.

CSV for Hazelcast Jet Operator listed in [OperatorHub.io](https://operatorhub.io/operator/hazelcast-jet-operator)
can be viewed from [here](https://github.com/operator-framework/community-operators/blob/master/upstream-community-operators/hazelcast-jet-operator/0.0.2/hazelcast-jet-operator.v0.0.2.clusterserviceversion.yaml).

## High Level Overview

The image above shows interactions between and user after the Hazelcast
Jet Operator has been deployed to the cluster. User submits the custom
resource with the type of `HazelcastJet` and Hazelcast Jet Operator
handles the request.

Based on the contents of the requests Operator either instantiates a new
cluster or reconciles the existing ones with the desired state.

![High Level Overview](/docs/assets/operator.svg)

## Certification

### RedHat Certified Operators

RedHat has a certification programme for Operators and certified
Operators will get published to the OpenShift catalog. The criteria for
certification can be found
[here](https://redhat-connect.gitbook.io/certified-operator-guide/what-if-ive-already-published-a-community-operator#certification-of-a-community-operator)

We've passed through this process for Hazelcast Jet Enterprise Operator
and it can be installed directly from the OpenShift catalog from the UI.

During the process we've created OpenShift specific docker images for
[Hazelcast Jet Enterprise](https://github.com/hazelcast/hazelcast-jet-docker/tree/master/openshift/hazelcast-jet-enterprise)
and [Hazelcast Jet Management Center](https://github.com/hazelcast/hazelcast-jet-management-center-docker/tree/master/openshift)
. Those images are built and published using RedHat Partner Connect
build service. Released versions of those images can be found in the
RedHat Container Catalog for [Hazelcast Jet Enterprise](https://access.redhat.com/containers/#/registry.connect.redhat.com/hazelcast/hazelcast-jet-enterprise-4)
and [Hazelcast Jet Management Center](https://access.redhat.com/containers/#/registry.connect.redhat.com/hazelcast/hazelcast-jet-management-center-4).

## Testing Details

Smoke testing has been done on our OpenShift on-premise test lab for the
certified operator.

For community/non-certified versions of the operators which gets
published to the [OperatorHub.io](https://operatorhub.io/) are tested
automatically with a corresponding Jenkins pipeline which
deploys them to a regular Kubernetes cluster and verifies the cluster
formation.

Automated test pipeline, which creates a Hazelcast Jet cluster with
Management Center using OpenShift specific images then verifies its
health, has been added to the Jenkins.

## Future Improvements

It is logical to extend capabilities of the operators to cover more
scenarios rather than the basic installation.
