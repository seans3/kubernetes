<!--
**Note:** When your KEP is complete, all of these comment blocks should be removed.

To get started with this template:

- [X] **Pick a hosting SIG.**
  Make sure that the problem space is something the SIG is interested in taking
  up. KEPs should not be checked in without a sponsoring SIG.
- [X] **Create an issue in kubernetes/enhancements**  (Issue #4006).
  When filing an enhancement tracking issue, please make sure to complete all
  fields in that template. One of the fields asks for a link to the KEP. You
  can leave that blank until this KEP is filed, and then go back to the
  enhancement and add the link.
- [X] **Make a copy of this template directory.**
  Copy this template into the owning SIG's directory and name it
  `NNNN-short-descriptive-title`, where `NNNN` is the issue number (with no
  leading-zero padding) assigned to your enhancement above.
- [X] **Fill out as much of the kep.yaml file as you can.**
  At minimum, you should fill in the "Title", "Authors", "Owning-sig",
  "Status", and date-related fields.
- [X] **Fill out this file as best you can.**
  At minimum, you should fill in the "Summary" and "Motivation" sections.
  These should be easy if you've preflighted the idea of the KEP with the
  appropriate SIG(s).
- [X] **Create a PR for this KEP.**
  Assign it to people in the SIG who are sponsoring this process.
- [ ] **Merge early and iterate.**
  Avoid getting hung up on specific details and instead aim to get the goals of
  the KEP clarified and merged quickly. The best way to do this is to just
  start with the high-level sections and fill out details incrementally in
  subsequent PRs.

Just because a KEP is merged does not mean it is complete or approved. Any KEP
marked as `provisional` is a working document and subject to change. You can
denote sections that are under active debate as follows:

```
<<[UNRESOLVED optional short context or usernames ]>>
Stuff that is being argued.
<<[/UNRESOLVED]>>
```

When editing KEPS, aim for tightly-scoped, single-topic PRs to keep discussions
focused. If you disagree with what is already in a document, open a new PR
with suggested changes.

One KEP corresponds to one "feature" or "enhancement" for its whole lifecycle.
You do not need a new KEP to move from beta to GA, for example. If
new details emerge that belong in the KEP, edit the KEP. Once a feature has become
"implemented", major changes should get new KEPs.

The canonical place for the latest set of instructions (and the likely source
of this file) is [here](/keps/NNNN-kep-template/README.md).

**Note:** Any PRs to move a KEP to `implementable`, or significant changes once
it is marked `implementable`, must be approved by each of the KEP approvers.
If none of those approvers are still appropriate, then changes to that list
should be approved by the remaining approvers and/or the owning SIG (or
SIG Architecture for cross-cutting KEPs).
-->
# KEP-4006: Transition from SPDY to WebSockets

<!--
This is the title of your KEP. Keep it short, simple, and descriptive. A good
title can help communicate what the KEP is and should be considered as part of
any review.
-->

<!--
A table of contents is helpful for quickly jumping to sections of a KEP and for
highlighting any additional information provided beyond the standard KEP
template.

Ensure the TOC is wrapped with
  <code>&lt;!-- toc --&rt;&lt;!-- /toc --&rt;</code>
tags, and then generate with `hack/update-toc.sh`.
-->

<!-- toc -->
- [Release Signoff Checklist](#release-signoff-checklist)
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [User Stories (Optional)](#user-stories-optional)
    - [Story 1](#story-1)
    - [Story 2](#story-2)
  - [Notes/Constraints/Caveats (Optional)](#notesconstraintscaveats-optional)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [Test Plan](#test-plan)
      - [Prerequisite testing updates](#prerequisite-testing-updates)
      - [Unit tests](#unit-tests)
      - [Integration tests](#integration-tests)
      - [e2e tests](#e2e-tests)
  - [Graduation Criteria](#graduation-criteria)
  - [Upgrade / Downgrade Strategy](#upgrade--downgrade-strategy)
  - [Version Skew Strategy](#version-skew-strategy)
- [Production Readiness Review Questionnaire](#production-readiness-review-questionnaire)
  - [Feature Enablement and Rollback](#feature-enablement-and-rollback)
  - [Rollout, Upgrade and Rollback Planning](#rollout-upgrade-and-rollback-planning)
  - [Monitoring Requirements](#monitoring-requirements)
  - [Dependencies](#dependencies)
  - [Scalability](#scalability)
  - [Troubleshooting](#troubleshooting)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Infrastructure Needed (Optional)](#infrastructure-needed-optional)
<!-- /toc -->

## Release Signoff Checklist

<!--
**ACTION REQUIRED:** In order to merge code into a release, there must be an
issue in [kubernetes/enhancements] referencing this KEP and targeting a release
milestone **before the [Enhancement Freeze](https://git.k8s.io/sig-release/releases)
of the targeted release**.

For enhancements that make changes to code or processes/procedures in core
Kubernetes—i.e., [kubernetes/kubernetes], we require the following Release
Signoff checklist to be completed.

Check these off as they are completed for the Release Team to track. These
checklist items _must_ be updated for the enhancement to be released.
-->

Items marked with (R) are required *prior to targeting to a milestone / release*.

- [ ] (R) Enhancement issue in release milestone, which links to KEP dir in [kubernetes/enhancements] (not the initial KEP PR)
- [ ] (R) KEP approvers have approved the KEP status as `implementable`
- [ ] (R) Design details are appropriately documented
- [ ] (R) Test plan is in place, giving consideration to SIG Architecture and SIG Testing input (including test refactors)
  - [ ] e2e Tests for all Beta API Operations (endpoints)
  - [ ] (R) Ensure GA e2e tests meet requirements for [Conformance Tests](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/conformance-tests.md) 
  - [ ] (R) Minimum Two Week Window for GA e2e tests to prove flake free
- [ ] (R) Graduation criteria is in place
  - [ ] (R) [all GA Endpoints](https://github.com/kubernetes/community/pull/1806) must be hit by [Conformance Tests](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/conformance-tests.md) 
- [ ] (R) Production readiness review completed
- [ ] (R) Production readiness review approved
- [ ] "Implementation History" section is up-to-date for milestone
- [ ] User-facing documentation has been created in [kubernetes/website], for publication to [kubernetes.io]
- [ ] Supporting documentation—e.g., additional design documents, links to mailing list discussions/SIG meetings, relevant PRs/issues, release notes

<!--
**Note:** This checklist is iterative and should be reviewed and updated every time this enhancement is being considered for a milestone.
-->

[kubernetes.io]: https://kubernetes.io/
[kubernetes/enhancements]: https://git.k8s.io/enhancements
[kubernetes/kubernetes]: https://git.k8s.io/kubernetes
[kubernetes/website]: https://git.k8s.io/website

## Summary

<!--
This section is incredibly important for producing high-quality, user-focused
documentation such as release notes or a development roadmap. It should be
possible to collect this information before implementation begins, in order to
avoid requiring implementors to split their attention between writing release
notes and implementing the feature itself. KEP editors and SIG Docs
should help to ensure that the tone and content of the `Summary` section is
useful for a wide audience.

A good summary is probably at least a paragraph in length.

Both in this section and below, follow the guidelines of the [documentation
style guide]. In particular, wrap lines to a reasonable length, to make it
easier for reviewers to cite specific portions, and to minimize diff churn on
updates.

[documentation style guide]: https://github.com/kubernetes/community/blob/master/contributors/guide/style-guide.md
-->

Some Kubernetes clients need to communicate with the API Server using a bi-directional
streaming protocol, instead of the standard HTTP request/response mechanism. A streaming
protocol provides the ability read and write arbitrary data messages between the
client and server, instead of providing a single response to a client request.
For example, the commands `kubectl exec`, `kubectl attach`, `kubectl port-forward`,
and `kubectl cp` all benefit from a bi-directional streaming protocol. Currently,
the bi-directional streaming solution for these `kubectl` commands is SPDY/3.1. For
the communication leg between `kubectl` and the API Server, this enhancement transitions
the bi-directional streaming protocol to WebSockets from SPDY/3.1 for three `kubectl`
commands: `exec`, `attach`, and `cp`.

## Motivation

<!--
This section is for explicitly listing the motivation, goals, and non-goals of
this KEP.  Describe why the change is important and the benefits to users. The
motivation section can optionally provide links to [experience reports] to
demonstrate the interest in a KEP within the wider Kubernetes community.

[experience reports]: https://github.com/golang/go/wiki/ExperienceReports
-->

The SPDY streaming protocol has been deprecated for around eight years, and by now
many proxies, gateways, and load-balancers do not support SPDY. Our effort to modernize
the streaming protocol between Kubernetes clients and the API Server is necessary
to enable the aforementioned intermediaries. Additionally, the better supported and
modern WebSockets should be more reliable and maintainable than SPDY.

### Goals

<!--
List the specific goals of the KEP. What is it trying to achieve? How will we
know that this has succeeded?
-->

1. Transition the bi-directional streaming protocol from SPDY/3.1 to WebSockets for
`kubectl exec`, `kubectl attach`, and `kubectl cp` for the communication leg
between `kubectl` and the API Server.

### Non-Goals

<!--
What is out of scope for this KEP? Listing non-goals helps to focus discussion
and make progress.
-->

1. We do not intend to initially transition the current SPDY bi-directional streaming
protocol for `kubectl port-forward`. This command requires a different subprotocol
than the previously stated three `kubectl` commands (which use the `RemoteCommand`
subprotocol). We intend to transition `kubectl port-forward` in a future release.

2. We do not initially intend to modify *any* of the SPDY communication paths between
other cluster components other than `kubectl` and the API Server. In other words,
the current SPDY communication paths between the API Server and Kubelet, or Kubelet
and the Container Runtime will not change.

3. We will not make *any* changes to current WebSocket based browser/javascript clients.

## Proposal

<!--
This is where we get down to the specifics of what the proposal actually is.
This should have enough detail that reviewers can understand exactly what
you're proposing, but should not include things like API designs or
implementation. What is the desired outcome and how do we measure success?.
The "Design Details" section below is for the real
nitty-gritty.
-->

Currently, the bi-directional streaming protocols (either SPDY or WebSockets) are 
initiated from clients, proxied by the API Server and Kubelet, and terminated at
the Container Runtime (e.g. containerd or CRI-O). This enhancement proposes to 1)
modify `kubectl` to request a WebSocket based streaming connection, and to 2) modify
the current API Server proxy to translate the `kubectl` WebSockets data stream to
a SPDY upstream connection. In this way, the cluster components upstream from the
API Server will not need to be changed.

### User Stories (Optional)

<!--
Detail the things that people will be able to do if this KEP is implemented.
Include as much detail as possible so that people can understand the "how" of
the system. The goal here is to make this feel real for users without getting
bogged down.
-->

<!--
User stories are not added, since this functionality should be transparent
to users.
-->

N/A

### Notes/Constraints/Caveats (Optional)

<!--
What are the caveats to the proposal?
What are some important details that didn't come across above?
Go in to as much detail as necessary here.
This might be a good place to talk about core concepts and how they relate.
-->

N/A

### Risks and Mitigations

<!--
What are the risks of this proposal, and how do we mitigate? Think broadly.
For example, consider both security and how this will impact the larger
Kubernetes ecosystem.

How will security be reviewed, and by whom?

How will UX be reviewed, and by whom?

Consider including folks who also work outside the SIG or subproject.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

## Design Details

<!--
This section should contain enough information that the specifics of your
change are understandable. This may include API specs (though not always
required) or even code snippets. If there's any ambiguity about HOW your
proposal will be implemented, this is the place to discuss them.
-->

**Current SPDY Streaming Architectural Diagram**

![Current SPDY Streaming Architectural Diagram](./spdy-architechtural-diagram.png)

### Background: Streaming Protocol Basics

`kubectl` bi-directional streaming connections are by created by upgrading an
initial HTTP/1.1 request. By adding two headers (`Connection: Upgrade`, `Upgrade: SPDY/3.1`),
the request can initiate the streaming upgrade. And when the response returns status
`101 Switching Protocols` signalling success, the connection can then be kept open
for subsequent streaming. An example of an upgraded HTTP Request/Response for `kubectl exec`
could look like:

**HTTP Request**
```
POST /api/v1/…/pods/nginx/exec?command=<CMD>... HTTP/1.1
Connection: Upgrade
Upgrade: SPDY/3.1
X-Stream-Protocol-Version: v4.channel.k8s.io
X-Stream-Protocol-Version: v3.channel.k8s.io
X-Stream-Protocol-Version: v2.channel.k8s.io
X-Stream-Protocol-Version: v1.channel.k8s.io
```

**HTTP Response**
```
HTTP/1.1 101 Switching Protocols
Connection: Upgrade
Upgrade: SPDY/3.1
X-Stream-Protocol-Version: v4.channel.k8s.io
```

If the upgrade is successful, one of the requested subprotocol versions is chosen
and returned in the response. In this instance, the chosen version of the subprotocol
is: `v4.channel.k8s.io`.

### Background: `RemoteCommand` Subprotocol

![Remote Command Subprotocol](./remote-command-subprotocol.png)

Once the connection is upgraded to a bi-directional streaming connection, the
client and server can exchange data messages. These messages are interpreted with
agreed upon standards which are called subprotocols. The three `kubectl` commands
(`exec`, `attach`, and `cp`) communicate using the `RemoteCommand` subprotocol. Basically,
this subprotocol provides command line functionality from the client to a running
container in the cluster. By multiplexing `stdin`, `stdout`, `stderr`, and `tty`
resizing over a streaming connection, this subprotocol supports clients executing
and interacting with commands executed on a container in the cluster. An example of
`kubectl exec` running the `date` command on an `nginx` pod/container is:

```
$ kubectl exec nginx -- date
Tue May 16 03:34:04 PM PDT 2023
```

### Background: API Server and Kubelet `UpgradeAwareProxy`

In order to route the data streamed between the client and the container, both the
API Server and Kubelet must proxy these data messages. Both the API Server and the
Kubelet provide this functionality with the `UpgradeAwareProxy`, which is a reverse
proxy that knows how to deal with the connection upgrade handshake.

### Proposal: `kubectl` WebSocket Executor and Fallback Executor

This enhancement proposes adding a `WebSocketExecutor` to `kubectl`, implementing
the WebSocket client using a new subprotol version (`v5.channel.k8s.io`). Additionally,
we propose creating a `FallbackExecutor` to address client/server version skew. The
`FallbackExecutor`  first attempts to upgrade the connection to WebSockets
version five (`v5.channel.k8s.io`) with the `WebSocketExecutor`, then falls back
to the legacy `SPDYExecutor` version four (`v4.channel.k8s.io`), if the upgrade is
unsuccessful. Note that this mechanism can require two request/response trips instead
of one. While the fallback mechanism may require an extra request/response if the
initial upgrade is not successful, we believe this possible extra roundtrip is justified
for the following reasons:

1. The upgrade handshake is implemented in low-level SPDY and WebSocket libraries,
and it is not easily exposed by these libraries. If it is even possible to modify
the upgrade handshake, the added complexity would not be worth the effort.
2. The streaming is already IO heavy, so another roundtrip will not substantially
affect the perceived performance.
3. As releases increment, the probablity of a WebSocket enabled `kubectl` communicating
with an older non-WebSocket enabled API Server decreases.

### Proposal: `v5.channel.k8s.io` subprotocol version

As previously mentioned, we propose incrementing the subprotocol version from four
to five: `v5.channel.k8s.io`. This version will indicate to upstream components
that `kubectl` is interested in using WebSockets between the client and the API Server
if it is supported.

### Proposal: API Server `StreamTranslatorProxy`

![Stream Translator Proxy](./stream-translator-proxy-2.png)

Currently, the API Server role within client/container streaming is to proxy the
data stream using the `UpgradeAwareProxy`. This enhancement proposes to modify the
SPDY data stream between `kubectl` and the API Server by conditionally adding a
`StreamTranslatorProxy` at the API Server. If the request is for a WebSocket upgrade
of version `v5.channel.k8s.io`, the `UpgradeAwareProxy` will delegate to the
`StreamTranslatorProxy`. This translation proxy terminates the WebSocket connection,
and de-multiplexes the various streams in order to pass the data on to a SPDY connection,
which continues upstream (to Kubelet and eventually the container runtime).

### Test Plan

<!--
**Note:** *Not required until targeted at a release.*
The goal is to ensure that we don't accept enhancements with inadequate testing.

All code is expected to have adequate tests (eventually with coverage
expectations). Please adhere to the [Kubernetes testing guidelines][testing-guidelines]
when drafting this test plan.

[testing-guidelines]: https://git.k8s.io/community/contributors/devel/sig-testing/testing.md
-->

[X] I/we understand the owners of the involved components may require updates to
existing tests to make this code solid enough prior to committing the changes necessary
to implement this enhancement.

##### Prerequisite testing updates

<!--
Based on reviewers feedback describe what additional tests need to be added prior
implementing this enhancement to ensure the enhancements have also solid foundations.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

##### Unit tests

<!--
In principle every added code should have complete unit test coverage, so providing
the exact set of tests will not bring additional value.
However, if complete unit test coverage is not possible, explain the reason of it
together with explanation why this is acceptable.
-->

<!--
Additionally, for Alpha try to enumerate the core package you will be touching
to implement this enhancement and provide the current unit coverage for those
in the form of:
- <package>: <date> - <current test coverage>
The data can be easily read from:
https://testgrid.k8s.io/sig-testing-canaries#ci-kubernetes-coverage-unit

This can inform certain test coverage improvements that we want to do before
extending the production code to implement this enhancement.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

- `<package>`: `<date>` - `<test coverage>`

##### Integration tests

<!--
Integration tests are contained in k8s.io/kubernetes/test/integration.
Integration tests allow control of the configuration parameters used to start the binaries under test.
This is different from e2e tests which do not allow configuration of parameters.
Doing this allows testing non-default options and multiple different and potentially conflicting command line options.
-->

<!--
This question should be filled when targeting a release.
For Alpha, describe what tests will be added to ensure proper quality of the enhancement.

For Beta and GA, add links to added tests together with links to k8s-triage for those tests:
https://storage.googleapis.com/k8s-triage/index.html
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

- <test>: <link to test coverage>

##### e2e tests

<!--
This question should be filled when targeting a release.
For Alpha, describe what tests will be added to ensure proper quality of the enhancement.

For Beta and GA, add links to added tests together with links to k8s-triage for those tests:
https://storage.googleapis.com/k8s-triage/index.html

We expect no non-infra related flakes in the last month as a GA graduation criteria.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

- <test>: <link to test coverage>

### Graduation Criteria

<!--
**Note:** *Not required until targeted at a release.*

Define graduation milestones.

These may be defined in terms of API maturity, [feature gate] graduations, or as
something else. The KEP should keep this high-level with a focus on what
signals will be looked at to determine graduation.

Consider the following in developing the graduation criteria for this enhancement:
- [Maturity levels (`alpha`, `beta`, `stable`)][maturity-levels]
- [Feature gate][feature gate] lifecycle
- [Deprecation policy][deprecation-policy]

Clearly define what graduation means by either linking to the [API doc
definition](https://kubernetes.io/docs/concepts/overview/kubernetes-api/#api-versioning)
or by redefining what graduation means.

In general we try to use the same stages (alpha, beta, GA), regardless of how the
functionality is accessed.

[feature gate]: https://git.k8s.io/community/contributors/devel/sig-architecture/feature-gates.md
[maturity-levels]: https://git.k8s.io/community/contributors/devel/sig-architecture/api_changes.md#alpha-beta-and-stable-versions
[deprecation-policy]: https://kubernetes.io/docs/reference/using-api/deprecation-policy/

Below are some examples to consider, in addition to the aforementioned [maturity levels][maturity-levels].

#### Alpha

- Feature implemented behind a feature flag
- Initial e2e tests completed and enabled

#### Beta

- Gather feedback from developers and surveys
- Complete features A, B, C
- Additional tests are in Testgrid and linked in KEP

#### GA

- N examples of real-world usage
- N installs
- More rigorous forms of testing—e.g., downgrade tests and scalability tests
- Allowing time for feedback

**Note:** Generally we also wait at least two releases between beta and
GA/stable, because there's no opportunity for user feedback, or even bug reports,
in back-to-back releases.

**For non-optional features moving to GA, the graduation criteria must include
[conformance tests].**

[conformance tests]: https://git.k8s.io/community/contributors/devel/sig-architecture/conformance-tests.md

#### Deprecation

- Announce deprecation and support policy of the existing flag
- Two versions passed since introducing the functionality that deprecates the flag (to address version skew)
- Address feedback on usage/changed behavior, provided on GitHub issues
- Deprecate the flag
-->

#### Alpha

```
<<[UNRESOLVED @seans3]>>
TBD - currently not finished
<<[/UNRESOLVED]>>
```

- `WebSocketExecutor` and `FallbackExecutor` completed and functional behind a `kubectl` feature flag.
- `StreamTranslatorProxy` successfully integrated into the `UpgradeAwareProxy` behind an API Server feature flag.
- Initial unit tests completed and enabled
- Initial integration tests completed and enabled
- Initial e2e tests completed and enabled

### Upgrade / Downgrade Strategy

<!--
If applicable, how will the component be upgraded and downgraded? Make sure
this is in the test plan.

Consider the following in developing an upgrade/downgrade strategy for this
enhancement:
- What changes (in invocations, configurations, API use, etc.) is an existing
  cluster required to make on upgrade, in order to maintain previous behavior?
- What changes (in invocations, configurations, API use, etc.) is an existing
  cluster required to make on upgrade, in order to make use of the enhancement?
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

### Version Skew Strategy

<!--
If applicable, how will the component handle version skew with other
components? What are the guarantees? Make sure this is in the test plan.

Consider the following in developing a version skew strategy for this
enhancement:
- Does this enhancement involve coordinating behavior in the control plane and
  in the kubelet? How does an n-2 kubelet without this feature available behave
  when this feature is used?
- Will any other components on the node change? For example, changes to CSI,
  CRI or CNI may require updating that component before the kubelet.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

<!-- DISCUSS the FallbackExecutor and v5.channel.k8s.io to address version skew -->

## Production Readiness Review Questionnaire

<!--

Production readiness reviews are intended to ensure that features merging into
Kubernetes are observable, scalable and supportable; can be safely operated in
production environments, and can be disabled or rolled back in the event they
cause increased failures in production. See more in the PRR KEP at
https://git.k8s.io/enhancements/keps/sig-architecture/1194-prod-readiness.

The production readiness review questionnaire must be completed and approved
for the KEP to move to `implementable` status and be included in the release.

In some cases, the questions below should also have answers in `kep.yaml`. This
is to enable automation to verify the presence of the review, and to reduce review
burden and latency.

The KEP must have a approver from the
[`prod-readiness-approvers`](http://git.k8s.io/enhancements/OWNERS_ALIASES)
team. Please reach out on the
[#prod-readiness](https://kubernetes.slack.com/archives/CPNHUMN74) channel if
you need any help or guidance.
-->

### Feature Enablement and Rollback

<!--
This section must be completed when targeting alpha to a release.
-->

###### How can this feature be enabled / disabled in a live cluster?

<!--
Pick one of these and delete the rest.

Documentation is available on [feature gate lifecycle] and expectations, as
well as the [existing list] of feature gates.

[feature gate lifecycle]: https://git.k8s.io/community/contributors/devel/sig-architecture/feature-gates.md
[existing list]: https://kubernetes.io/docs/reference/command-line-tools-reference/feature-gates/
-->

```
<<[UNRESOLVED @seans3]>>
TBD - currently not finished
<<[/UNRESOLVED]>>
```

- [X] Feature gate (also fill in values in `kep.yaml`)
  - Feature gate name: WebSockets
  - Components depending on the feature gate: kubectl, API Server
- [ ] Other
  - Describe the mechanism: TODO
  - Will enabling / disabling the feature require downtime of the control
    plane? Yes. The API Server would have to be restarted to change the value of
	the feature flag. `kubectl`, however, would only need an environment variable
	to change the value of the feature flag, so it would not affect the control
	plane.
  - Will enabling / disabling the feature require downtime or reprovisioning
    of a node? No.

###### Does enabling the feature change any default behavior?

<!--
Any change of default behavior may be surprising to users or break existing
automations, so be extremely careful here.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

###### Can the feature be disabled once it has been enabled (i.e. can we roll back the enablement)?

<!--
Describe the consequences on existing workloads (e.g., if this is a runtime
feature, can it break the existing applications?).

Feature gates are typically disabled by setting the flag to `false` and
restarting the component. No other changes should be necessary to disable the
feature.

NOTE: Also set `disable-supported` to `true` or `false` in `kep.yaml`.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

###### What happens if we reenable the feature if it was previously rolled back?

###### Are there any tests for feature enablement/disablement?

<!--
The e2e framework does not currently support enabling or disabling feature
gates. However, unit tests in each component dealing with managing data, created
with and without the feature, are necessary. At the very least, think about
conversion tests if API types are being modified.

Additionally, for features that are introducing a new API field, unit tests that
are exercising the `switch` of feature gate itself (what happens if I disable a
feature gate after having objects written with the new field) are also critical.
You can take a look at one potential example of such test in:
https://github.com/kubernetes/kubernetes/pull/97058/files#diff-7826f7adbc1996a05ab52e3f5f02429e94b68ce6bce0dc534d1be636154fded3R246-R282
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

### Rollout, Upgrade and Rollback Planning

<!--
This section must be completed when targeting beta to a release.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

###### How can a rollout or rollback fail? Can it impact already running workloads?

<!--
Try to be as paranoid as possible - e.g., what if some components will restart
mid-rollout?

Be sure to consider highly-available clusters, where, for example,
feature flags will be enabled on some API servers and not others during the
rollout. Similarly, consider large clusters and how enablement/disablement
will rollout across nodes.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

###### What specific metrics should inform a rollback?

<!--
What signals should users be paying attention to when the feature is young
that might indicate a serious problem?
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

###### Were upgrade and rollback tested? Was the upgrade->downgrade->upgrade path tested?

<!--
Describe manual testing that was done and the outcomes.
Longer term, we may want to require automated upgrade/rollback tests, but we
are missing a bunch of machinery and tooling and can't do that now.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

###### Is the rollout accompanied by any deprecations and/or removals of features, APIs, fields of API types, flags, etc.?

<!--
Even if applying deprecation policies, they may still surprise some users.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

### Monitoring Requirements

<!--
This section must be completed when targeting beta to a release.

For GA, this section is required: approvers should be able to confirm the
previous answers based on experience in the field.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

###### How can an operator determine if the feature is in use by workloads?

<!--
Ideally, this should be a metric. Operations against the Kubernetes API (e.g.,
checking if there are objects with field X set) may be a last resort. Avoid
logs or events for this purpose.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

###### How can someone using this feature know that it is working for their instance?

<!--
For instance, if this is a pod-related feature, it should be possible to determine if the feature is functioning properly
for each individual pod.
Pick one more of these and delete the rest.
Please describe all items visible to end users below with sufficient detail so that they can verify correct enablement
and operation of this feature.
Recall that end users cannot usually observe component logs or access metrics.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

- [ ] Events
  - Event Reason: 
- [ ] API .status
  - Condition name: 
  - Other field: 
- [ ] Other (treat as last resort)
  - Details:

###### What are the reasonable SLOs (Service Level Objectives) for the enhancement?

<!--
This is your opportunity to define what "normal" quality of service looks like
for a feature.

It's impossible to provide comprehensive guidance, but at the very
high level (needs more precise definitions) those may be things like:
  - per-day percentage of API calls finishing with 5XX errors <= 1%
  - 99% percentile over day of absolute value from (job creation time minus expected
    job creation time) for cron job <= 10%
  - 99.9% of /health requests per day finish with 200 code

These goals will help you determine what you need to measure (SLIs) in the next
question.
-->

###### What are the SLIs (Service Level Indicators) an operator can use to determine the health of the service?

<!--
Pick one more of these and delete the rest.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

- [ ] Metrics
  - Metric name:
  - [Optional] Aggregation method:
  - Components exposing the metric:
- [ ] Other (treat as last resort)
  - Details:

###### Are there any missing metrics that would be useful to have to improve observability of this feature?

<!--
Describe the metrics themselves and the reasons why they weren't added (e.g., cost,
implementation difficulties, etc.).
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

### Dependencies

<!--
This section must be completed when targeting beta to a release.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

###### Does this feature depend on any specific services running in the cluster?

<!--
Think about both cluster-level services (e.g. metrics-server) as well
as node-level agents (e.g. specific version of CRI). Focus on external or
optional services that are needed. For example, if this feature depends on
a cloud provider API, or upon an external software-defined storage or network
control plane.

For each of these, fill in the following—thinking about running existing user workloads
and creating new ones, as well as about cluster-level services (e.g. DNS):
  - [Dependency name]
    - Usage description:
      - Impact of its outage on the feature:
      - Impact of its degraded performance or high-error rates on the feature:
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

### Scalability

<!--
For alpha, this section is encouraged: reviewers should consider these questions
and attempt to answer them.

For beta, this section is required: reviewers must answer these questions.

For GA, this section is required: approvers should be able to confirm the
previous answers based on experience in the field.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

###### Will enabling / using this feature result in any new API calls?

<!--
Describe them, providing:
  - API call type (e.g. PATCH pods)
  - estimated throughput
  - originating component(s) (e.g. Kubelet, Feature-X-controller)
Focusing mostly on:
  - components listing and/or watching resources they didn't before
  - API calls that may be triggered by changes of some Kubernetes resources
    (e.g. update of object X triggers new updates of object Y)
  - periodic API calls to reconcile state (e.g. periodic fetching state,
    heartbeats, leader election, etc.)
-->

The proposed design envisions a fallback mechanism when a new `kubectl` communicates
with an older API Server. The client will initially request an upgrade to WebSockets,
but it will fallback to the legacy SPDY if it is not supported. In this version
skew scenario where the client implements the new functionality but the server does
not, there is an extra request/response. Since bi-directional streaming already is
very IO intensive, this extra request/response should not be significant. Additionally,
as releases are incremented, the probability of the version skew will continually
decrease.

<!-- DISCUSS SPDY heartbeat versus WebSocket heartbeat; should be the same load -->

###### Will enabling / using this feature result in introducing new API types?

<!--
Describe them, providing:
  - API type
  - Supported number of objects per cluster
  - Supported number of objects per namespace (for namespace-scoped objects)
-->

No

###### Will enabling / using this feature result in any new calls to the cloud provider?

<!--
Describe them, providing:
  - Which API(s):
  - Estimated increase:
-->

No

###### Will enabling / using this feature result in increasing size or count of the existing API objects?

<!--
Describe them, providing:
  - API type(s):
  - Estimated increase in size: (e.g., new annotation of size 32B)
  - Estimated amount of new objects: (e.g., new Object X for every existing Pod)
-->

```
<<[UNRESOLVED]>>
TBD - currently not finished
<<[/UNRESOLVED]>>
```

<!-- DESCRIBE the data framing for WebSocket (which is less than) SPDY. -->

###### Will enabling / using this feature result in increasing time taken by any operations covered by existing SLIs/SLOs?

<!--
Look at the [existing SLIs/SLOs].

Think about adding additional work or introducing new steps in between
(e.g. need to do X to start a container), etc. Please describe the details.

[existing SLIs/SLOs]: https://git.k8s.io/community/sig-scalability/slos/slos.md#kubernetes-slisslos
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

###### Will enabling / using this feature result in non-negligible increase of resource usage (CPU, RAM, disk, IO, ...) in any components?

<!--
Things to keep in mind include: additional in-memory state, additional
non-trivial computations, excessive access to disks (including increased log
volume), significant amount of data sent and/or received over network, etc.
This through this both in small and large cases, again with respect to the
[supported limits].

[supported limits]: https://git.k8s.io/community//sig-scalability/configs-and-limits/thresholds.md
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

###### Can enabling / using this feature result in resource exhaustion of some node resources (PIDs, sockets, inodes, etc.)?

<!--
Focus not just on happy cases, but primarily on more pathological cases
(e.g. probes taking a minute instead of milliseconds, failed pods consuming resources, etc.).
If any of the resources can be exhausted, how this is mitigated with the existing limits
(e.g. pods per node) or new limits added by this KEP?

Are there any tests that were run/should be run to understand performance characteristics better
and validate the declared limits?
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

### Troubleshooting

<!--
This section must be completed when targeting beta to a release.

For GA, this section is required: approvers should be able to confirm the
previous answers based on experience in the field.

The Troubleshooting section currently serves the `Playbook` role. We may consider
splitting it into a dedicated `Playbook` document (potentially with some monitoring
details). For now, we leave it here.
-->

###### How does this feature react if the API server and/or etcd is unavailable?

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

###### What are other known failure modes?

<!--
For each of them, fill in the following information by copying the below template:
  - [Failure mode brief description]
    - Detection: How can it be detected via metrics? Stated another way:
      how can an operator troubleshoot without logging into a master or worker node?
    - Mitigations: What can be done to stop the bleeding, especially for already
      running user workloads?
    - Diagnostics: What are the useful log messages and their required logging
      levels that could help debug the issue?
      Not required until feature graduated to beta.
    - Testing: Are there any tests for failure mode? If not, describe why.
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

###### What steps should be taken if SLOs are not being met to determine the problem?

## Implementation History

<!--
Major milestones in the lifecycle of a KEP should be tracked in this section.
Major milestones might include:
- the `Summary` and `Motivation` sections being merged, signaling SIG acceptance
- the `Proposal` section being merged, signaling agreement on a proposed design
- the date implementation started
- the first Kubernetes release where an initial version of the KEP was available
- the version of Kubernetes where the KEP graduated to general availability
- when the KEP was retired or superseded
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

## Drawbacks

<!--
Why should this KEP _not_ be implemented?
-->

```
<<[UNRESOLVED]>>
TBD - currently unimplemented
<<[/UNRESOLVED]>>
```

## Alternatives

<!--
What other approaches did you consider, and why did you rule them out? These do
not need to be as detailed as the proposal, but should include enough
information to express the idea and why it was not acceptable.
-->

```
<<[UNRESOLVED]>>
TBD - currently not finished
<<[/UNRESOLVED]>>
```

<!--  DISCUSS why we're not using http/2.0. -->


## Infrastructure Needed (Optional)

<!--
Use this section if you need things from the project/SIG. Examples include a
new subproject, repos requested, or GitHub details. Listing these here allows a
SIG to get the process for these resources started right away.
-->

N/A
