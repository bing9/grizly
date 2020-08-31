# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added - 31-05-2020
- Jupyterlab extensions (table of content, dask, jupyterlab-git)

## [0.2.2](https://github.com/tedcs/platform/compare/v0.2.1...v0.2.2) - 29-05-2020
### Added
- local Dask cluster is now working and accepts requests from grizly CLI (eg. `grizly workflow run "Sales Daily" --local`)*
- Docker user's TE ID is now available under the `DOCKER_USER` env variable

### Changed
- bumped `workflows` from `v0.1.2` to `v0.2.0`
- bumped `grizly` from `v0.3.3` to `v0.3.4`

*note the CLI will be available in the next grizly release

## [0.2.1](https://github.com/tedcs/platform/compare/v0.2...v0.2.1) - 12-05-2020
### Added
- added a stub for documentation

### Changed
- bumped `workflows` from `v0.1.1` to `v0.1.2`
- changed changelog.md to CHANGELOG.md and updated format to follow [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) guidelines

## [0.2](https://github.com/tedcs/platform/compare/v0.1...v0.2) - 08-05-2020
### Added
- added dev requirements
- added changelog

### Changed
- changed `grizly` and `workflows` to fixed versions:
  - grizly: 0.3.3
  - workflows: 0.1.1
- changed `python3.8` and `pip3.8` calls to `python3` and `pip3`
- changed `cloudpickle` requirement to a fixed version due to compatibility issues with EC2
