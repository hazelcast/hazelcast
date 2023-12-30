<!--
Contributing to Hazelcast and looking for a challenge? Why don't you check out our open positions?

https://hazelcast.pinpointhq.com/
-->

INSERT_PR_DESCRIPTION_HERE

Fixes INSERT_LINK_TO_THE_ISSUE_HERE

Backport of: INSERT_LINK_TO_THE_ORIGINAL_PR_HERE

Breaking changes (list specific methods/types/messages):
* API
* client protocol format
* serialized form
* snapshot format

Checklist:
- [ ] Labels (`Team:`, `Type:`, `Source:`, `Module:`) and Milestone set
- [ ] Add `Add to Release Notes` label if changes should be mentioned in release notes or `Not Release Notes content` if changes are not relevant for release notes
- [ ] Request reviewers if possible
- [ ] New public APIs have `@Nonnull/@Nullable` annotations
- [ ] New public APIs have `@since` tags in Javadoc
- [ ] Send backports/forwardports if fix needs to be applied to past/future releases
