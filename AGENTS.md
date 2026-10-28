# librptadvecholink development rules

## Shared rpt_advanced project baseline

This baseline applies to every production, shared-library, and workflow
repository in the rpt_advanced project. Repository-specific rules may add
constraints but must not weaken it.

Before a push, run platform-independent formatting, lint, and static
analysis—including Cppcheck—without rewriting source files. Do not run
Cppcheck in each platform job. GitHub repeats only those fast checks for an
ordinary push.

The full quality gate runs for every pull request and must pass before that
pull request can merge. It runs platform-independent formatting, lint, static
analysis, and Doxygen once, concurrently where independent; then it runs
platform-dependent build, tests, packaging, and staged-install checks
concurrently on native Debian 13 amd64 and arm64. Require 100% line and branch
coverage of production code only on Debian 13 amd64; test code is excluded from
coverage. Debian 12 support is aspirational: do not run automated Debian 12
tests or build Debian 12 packages as part of ordinary pushes, pull requests, or
releases. Build Debian 12 packages manually only when explicitly requested.
Automated releases publish Debian 13 packages only; node installations use
Debian 13 arm64 packages. A release uses a main revision that has already
passed the pull-request gate and performs only release-artifact validation; it
does not repeat the full gate.

Local recovery commits may follow affected targeted checks, but must not be
represented as fully verified until the pull-request gate passes.
Treat compiler warnings as errors and fail applicable formatting, Ruff,
ShellCheck, Cppcheck, Clang-Tidy, Doxygen, tests, installation checks, and 100%
line and branch coverage of production code on Debian 13 amd64. Remove
unreachable or dead code instead of suppressing diagnostics or excluding it
from coverage.

Update concise developer-facing in-source documentation, tests, and affected
build, install, and package artifacts before every implementation commit.
Defer user-facing documentation—manuals, examples, and other operator-facing
material—until immediately before creating a pull request. Consumers of a
shared project library must use its released, versioned dynamic shared object
rather than vendor or statically link a duplicate implementation.
Preserve published ABI/API compatibility whenever practical; when a change is
necessary, document its compatibility, SONAME/package consequences, and
migration. Start and clean only project-owned, labeled test containers
deterministically. Never deploy to a node or alter its configuration without
explicit approval.

## Component boundary

Implement only the approved EchoLink plan; preserve existing controller and radio behavior.
Use Rustdoc for Rust developer documentation and Doxygen for the C ABI.
This portable library must not depend on Asterisk or hardware transports.
Audio operations must not allocate, lock, block, log, or perform I/O.
Keep descriptor, SONAME, Debian packages, pkg-config, and staged-install checks
synchronized. Read the applicable ADRs in rpt_advanced before boundary changes.
Workflow implementations live in rptadv-shared-library-workflows; this
repository contains only thin callers. Never deploy without explicit approval.
