<PR description here>

Fixes #NNNN (point out issues this PR fixes, if any)

Forward-port (backport) of: #NNNN (link to PR which is the basis for this PR, if applicable)

EE PR: #NNNN (link to enterprise counterpart PR)

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
