# pbs-fast-gc

A slightly faster gc tool for your proxmox-backup-server

## How it works

scanning indexes and chunks in parallel.

## How to use

1. build binary or download from release
2. Wait till all proxmox backup is finished
3. (optional) Set the proxmox backup storage to read-only
4. run `./pbs-fast-gc -baseDir /path/to/your/proxmox-backup/storage`
5. review the output (?)
6. run `./pbs-fast-gc -baseDir /path/to/your/proxmox-backup/storage -delete` to actually delete the unreferenced chunks

## GC Speed

| filesystem         | size | Time                    |
| ------------------ | ---- | ----------------------- |
| juicefs with redis | 30T  | scan 2min, delete < 10m |
| ext4 on sdd        | 9T   | scan 1min, delete < 1m  |