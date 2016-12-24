# seekmin

Basic reimplementation of `md5sum` that computes hashes in parallel.

The naive way to run `md5sum` in parallel is to run multiple processes, e.g. via `xargs`.  Example:

    find /path/to/files -type f -print0 | xargs -0 -P 2 md5sum
    
where the `-P 2` argument to `xarg` causes two `md5sum` processes to run in parallel.

The problem with this naive approach is that the multiple `md5sum` processes issue reads concurrently.  On hard disks (but not SSDs) the concurrent reads cause extra seeks and can reduce the disk read throughput.

The `seekmin` implementation allows parallel hashing while keeping sequential reading.
