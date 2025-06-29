# Features And Improvements

## CLI / Workspace

### Include / Exclude

- [ ] Make lib.Pattern match like `.gitignore` does.
- [ ] Add option to honor .gitignore wherever it occurs.
- [ ] Store includes and excludes in the workspace config directory (`.cling`).
- [ ] Exclude overrides with includes should only match if the include match comes after the exclude match.
      For example, if exclude is `**/node_modules` and include is `**/test`, then `test/node_modules`
      should be excluded but `node_modules/test` should be included. Currently, both are included.

### Ignoring Errors

- [x] When committing, we should not ignore errors because that would lead to the paths being
      removed from the repository. The user should exclude the paths explicitly.
      (We don't ever ignore errors anymore.)
