# Release Checklist

This document outlines how to release a new version of Blip.
It is not the [release notes](https://block.github.io/blip/about/release-notes).

Review Blip [versioning](https://github.com/cashapp/blip/blob/main/CONTRIBUTING.md#versioning) guidelines.

## 1. Release Branch

First, create a branch to prepare the new version.

- [ ] Ensure local main branch is up to date and clean: `git co main && git pull`
- [ ] Create release branch: `git co -b v1.Y.Z` (replace Y and Z with new version)
- [ ] Bump version const [`blip.VERSION`](https://github.com/cashapp/blip/blob/main/blip.go#L21)

## 2. Documentation

Second, update the [documentation](https://block.github.io/blip/).
Run `docs/serve.sh` to edit locally.

- [ ] Write [release notes](https://block.github.io/blip/about/release-notes) for the new version (maintain style: past tense one liners)
- [ ] Check and update [Readiness](https://block.github.io/blip/ready)
- [ ] Update other pages affected by new changes (or ask contributors to update docs affected by their changes)
- [ ] Glance over all docs to make sure nothing obvious is broken or wrong (including HTML/CSS layout)

## 3. GitHub Release

Third, merge the release branch and create a GitHub release.

- [ ] Add, commit, and merge changes in the release branch; commit message "Release v1.Y.Z" or similar
- [ ] [Wait for GitHub Actions](https://github.com/cashapp/blip/actions) to build/publish
- [ ] Update local main branch: `git co main && git pull`
- [ ] Tag local main with new version: `git tag v1.Y.Z` (replace Y and Z)
- [ ] Push tag: `git push --tags`
- [ ] [Draft a new release](https://github.com/cashapp/blip/releases/new)
  - [ ] Select new version tag
  - [ ] Click "Generate release notes" button (on the right)
  - [ ] Put "Human-readable release notes: ..." preamble before generated release notes (see [v1.0.1 release](https://github.com/cashapp/blip/releases/tag/v1.0.1) for example)
  - [ ] Clean up generated releases that are noisy or useless
  - [ ] Make sure "Set as the latest release" is checked (leave pre-release unchecked/clear)
  - [ ] Pubish release

Congratulations and thank you for helping develop Blip and monitor MySQL!
