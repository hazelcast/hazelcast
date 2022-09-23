# Design document template

### Table of Contents

+ [Background](#background)
  - [Description](#description)
  - [Terminology](#terminology)
  - [Actors and Scenarios](#actors-and-scenarios)
+ [Functional Design](#functional-design)
  * [Summary of Functionality](#summary-of-functionality)
  * [Additional Functional Design Topics](#additional-functional-design-topics)
    + [Notes/Questions/Issues](#notesquestionsissues)
+ [User Interaction](#user-interaction)
  - [API design and/or Prototypes](#api-design-andor-prototypes)
+ [Client Related Changes](#client-related-changes)
+ [Technical Design](#technical-design)
+ [Testing Criteria](#testing-criteria)
+ [Other Artifacts](#other-artifacts)



|||
|---|---|
|Related Jira|_Jira Story/Task link_|
|Related Github issues|_GH issue list_|
|Document Status / Completeness|DRAFT / IN PROGRESS / DESIGN REVIEW / DONE|
|Requirement owner|_Requirement owner_|
|Developer(s)|_Implementor_|
|Quality Engineer|_Quality Engineer_|
|Support Engineer|_Support Engineer_|
|Technical Reviewers|_Technical Reviewers_|
|Simulator or Soak Test PR(s) |_Link to Simulator or Soak test PR_|

### Background
#### Description

- Provide a general description of the purpose of this design spec and a high-level summary of the functionality it covers. Suggested contents:
  - What is being proposed
  - Why (short reason)
  - How (short plan)
  - Impact
- Mention goals and non-goals
- Describe motivation
  - Why are we doing this?
  - What use cases does it support?
  - What is the expected outcome?
  - What is the impact of not doing this?
  - Add links to any related user stories to the proposed work. Specific customer names need not be included, for confidentiality, but it is still useful to describe their use cases.

#### Terminology

|Term|Definition|
|---|---|
|||

#### Actors and Scenarios

Provide a list of actors and the business function(s) they perform related to this feature.

### Functional Design
#### Summary of Functionality

Provide a list of functions user(s) can perform.

#### Additional Functional Design Topics

Provide functional design details that are pertinent to this specific design spec. Use the h3 heading tag to distinguish topic titles.

##### Notes/Questions/Issues

- Document notes, questions, and issues identified with this functional design topic.
- List drawbacks - why should we *not* do this? If applicable, list mitigating factors that may make each drawback acceptable. Investigate the consequences of the proposed change onto other areas
  of Hazelcast. If other features are impacted, especially UX, list this impact as a reason not to do the change. If possible, also investigate and suggest mitigating actions that would reduce the impact. You can for example consider additional validation testing, additional documentation or doc changes, new user research, etc.
- What parts of the design do you expect to resolve through the design process before this gets merged?
- What parts of the design do you expect to resolve through the implementation of this feature before stabilization?
- What related issues do you consider out of scope for this document that could be addressed in the future independently of the solution that comes out of this change?

Use the ⚠️ or ❓icon to indicate an outstanding issue or question, and use the ✅ or ℹ️ icon to indicate a resolved issue or question.

### User Interaction
#### API design and/or Prototypes

Listing of associated prototypes (latest versions) and any API design samples. How do we teach this?

Explain the proposal as if it was already included in the project and you were teaching it to an end-user, or a Hazelcast team member in a different project area.

Consider the following writing prompts:
- Which new concepts have been introduced to end-users? Can you provide examples for each?
- How would end-users change their apps or thinking to use the change?
- Are there new error messages introduced? Can you provide examples?
- Are there new deprecation warnings? Can you provide examples?
- How are clusters affected that were created before this change? Are there migrations to consider?

#### Client Related Changes
Please identify if any client code change is required. If so, please provide a list of client code changes.
The changes may include API changes, serialization changes or other client related code changes.

Please notify the APIs team if any change is documented in this section. 
The changes may need to be handled for non-Java clients as well.

### Technical Design

This is the technical portion of the design document. Explain the design in sufficient detail.

Important writing prompts follow. You do not need to answer them in this particular order, but we wish to find answers to them throughout your prose.

Some of these prompts may not be relevant to your design document; in which case you can spell out “this change does not affect ...” or answer “N/A” (not applicable) next to the question.

- Questions about the change:
  - What components in Hazelcast need to change? How do they change? This section outlines the implementation strategy: for each component affected, outline how it is changed.
  - Are there new abstractions introduced by the change? New concepts? If yes, provide definitions and examples.
  - How does this work in a on-prem deployment? How about on AWS and Kubernetes?
  - How does the change behave in mixed-version deployments? During a version upgrade? Which migrations are needed?
  - What are the possible interactions with other features or sub-systems inside Hazelcast? How does the behavior of other code change implicitly as a result of the changes outlined in the design document? (Provide examples if relevant.)
  - Is there other ongoing or recent work that is related? (Cross-reference the relevant design documents.)
  - What are the edge cases? What are example uses or inputs that we think are uncommon but are still possible and thus need to be handled? How are these edge cases handled? Provide examples.
  - What are the effect of possible mistakes by other Hazelcast team members trying to use the feature in their own code? How does the change impact how they will troubleshoot things?
  - Mention alternatives, risks and assumptions. Why is this design the best in the space of possible designs? What other designs have been considered and what is the rationale for not choosing them?
  - Add links to any similar functionalities by other vendors, similarities and differentiators

- Questions about performance:
  - Does the change impact performance? How?
  - How is resource usage affected for “large” loads? For example, what do we expect to happen when there are 100000 items/entries? 100000 data structures? 1000000 concurrent operations?
  - Also investigate the consequences of the proposed change on performance. Pay especially attention to the risk that introducing a possible performance improvement in one area can slow down another area in an unexpected way. Examine all the current "consumers" of the code path you are proposing to change and consider whether the performance of any of them may be negatively impacted by the proposed change. List all these consequences as possible drawbacks.

- Stability questions:
  - Can this new functionality affect the stability of a node or the entire cluster? How does the behavior of a node or a cluster degrade if there is an error in the implementation?
  - Can the new functionality be disabled? Can a user opt out? How? Can the user disable it from the Management Center?
  - Can the new functionality affect clusters which are not explicitly using it?
  - What testing and safe guards are being put in place to protect against unexpected problems?

- Security questions:
  - Does the change concern authentication or authorization logic? If so, mention this explicitly tag the relevant security-minded reviewer as reviewer to the design document.
  - Does the change create a new way to communicate data over the network?  What rules are in place to ensure that this cannot be used by a malicious user to extract confidential data?
  - Is there telemetry or crash reporting? What mechanisms are used to ensure no sensitive data is accidentally exposed?

- Observability and usage questions:
  - Is the change affecting asynchronous / background subsystems?
    - If so, how can users and our team observe the run-time state via tracing?
    - Which other inspection APIs exist?
      (In general, think about how your coworkers and users will gain access to the internals of the change after it has happened to either gain understanding during execution or troubleshoot problems.)

  - Are there new APIs, or API changes (either internal or external)?
    - How would you document the new APIs? Include example usage.
    - What are the other components or teams that need to know about the new APIs and changes?
    - Which principles did you apply to ensure the APIs are consistent with other related features / APIs? (Cross-reference other APIs that are similar or related, for comparison.)

  - Is the change visible to users of Hazelcast or operators who run Hazelcast clusters?
    - Are there any user experience (UX) changes needed as a result of this change?
    - Are the UX changes necessary or clearly beneficial? (Cross-reference the motivation section.)
    - Which principles did you apply to ensure the user experience (UX) is consistent with other related features? (Cross-reference other features that have related UX, for comparison.)
    - Which other engineers or teams have you polled for input on the proposed UX changes? Which engineers or team may have relevant experience to provide feedback on UX?
  - Is usage of the new feature observable in telemetry? If so, mention where in the code telemetry counters or metrics would be added.
  - What might be the valuable metrics that could be shown for this feature in Management Center?
  - Should this feature be configured, enabled/disabled or managed from the Management Center? How do you think your change affects Management Center?
  - Does the feature require or allow runtime changes to the member configuration (XML/YAML/programmatic)?
  - Are usage statistics for this feature reported in Phone Home? If not, why?

The section should return to the user stories in the motivations section, and explain more fully how the detailed proposal makes those stories work.

### Testing Criteria

Describe testing approach to developed functionality
- Soak testing for memory leaks and stable performance?
- Security related tests?
- Negative tests?
- Stress tests?
- Tests related to deployment on AWS or Kubernetes? See [Hazelcast Guides](https://guides.hazelcast.org/home/) for examples on deployments, [Connect To Hazelcast Running on Kubernetes from Outside](https://guides.hazelcast.org/kubernetes-external-client/) and [GH workflow](https://github.com/hazelcast-guides/kubernetes-external-client/blob/main/.github/workflows/integrational-tests.yml) for example of automated tests and [Create automated tests in Kubernetes/OpenShift for enterprise features](https://hazelcast.atlassian.net/browse/CN-150) for JIRA task on automated testing of EE features. For more information about testing in kubernetes please see [here](https://guides.hazelcast.org/kubernetes/) and [here](https://docs.hazelcast.com/hazelcast/5.0/deploy/deploying-in-kubernetes), for testing in AWS, GCP and Azure please see [here](https://guides.hazelcast.org/terraform-quickstarts/).

Provide references to Testlink or Testing artifacts.

### Other Artifacts

Links to additional artifacts go here.