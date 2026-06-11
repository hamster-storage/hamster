// Package compat is the third-party client compatibility suite: the real
// aws CLI, rclone, restic, and s3cmd run against an in-process gateway,
// making the acceptance bar in docs/S3-API.md executable. Each tool gets its
// own test file, so the verbose test output reads as a what-works matrix.
//
// The suite is excluded from `task test` by the `compat` build tag: it
// shells out to external binaries with real clocks and real sockets, the
// opposite of the hermetic main suite. Run it with `task compat`. A tool
// that is not installed skips its tests rather than failing them — an
// absent binary is a gap in the local toolbox, not a compatibility bug.
package compat
