# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## To Do
- `JobList`, `ProcessList` as iterators
- Naming consistency: processing -> retrieve
- Remove unused `Remote.build_status_info` method
- Details for `Remote.status`
- Documentation
- Use pydantic for requests and responses
- CLI
- Architecture consistent with underlying API

## [Unreleased]

## [0.4.0] - 2023-06-14
### Added
- Support for multiple requests to the CADS API client [COPDS-1112](https://jira.ecmwf.int/browse/COPDS-1112)
- `Remote.response` property added (cached when job status is `successful` or `failed`)

### Changed
- `Remote.wait_on_result` returns status

### Changed
### Fixed
### Removed


[Unreleased]: https://github.com/ecmwf-projects/cads-api-client/compare/v0.4.0...main
[0.4.0]: https://github.com/ecmwf-projects/cads-api-client/compare/v0.3.0...v0.4.0

