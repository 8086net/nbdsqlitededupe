# NBD SQLite Dedupe

An NBD (Network Block Device) server with SQLite backend and block deduplication.

# What?

nbdsqlitededupe is a Python based NBD server built using nbdkit and SQLite for storage. 

# Why?

Following on from the Compress Blocks [cb](https://github.com/burtyb/cb) tool I use for moving ClusterCTRL images around I wanted something for online access to Pi disk images allowing quick loop mounting to peek inside without extracting archives or using lots of disk space. Previously I'd been using VDO to store images but that becomes a pain when moving to non-RH based distributions and AIUI deduplication only happens against recent writes.

# How to use?

## Debian

Install required packages
```
sudo apt install python3 nbdkit nbdkit-plugin-python nbd-client sqlite3
```

Start nbd-server
```
# Start the server in the foreground showing debug messages
# To background remove "-f" and to disable debug messges remove "-v"
# db = SQLite database filename
# size = size in bytes of exported NBD device
nbdkit -i localhost -v -f python3 /path/to/nbdsqlitededupe.py db=/path/to/database.sqlite3 size=1T
# Load the NBD kernel module
modprobe nbd max_part=8
# Connect to the NBD server
sudo nbd-client -b 4096 localhost /dev/nbd0
```

/dev/nbd0 can then be partitioned, filesystem created (on /dev/nbd0p1) and mounted as normal (remembering to use trim/discard if supported by the filesystem).

To shutdown NBD you first need to unmount the filesystem.
```
# Disconnect from the NBD server
nbd-client -d /dev/nbd0
# You can then CTRL-C the "nbdkit" process to stop it.
```

# Under the hood

Within the nbdsqlitededupe.py file there is a "trustHash" option, when set to False (default) when writing blocks both the SHA256 hash and the full 4096 bytes are compared to determine if the block is the same. When set to True only the SHA256 hash is compared which is quicker but a hash collision will cause corruption.

The SQLite database has two tables.

The "block" table stores the actual data of the 4096 byte blocks and the SHA256 hash.

The "mapper" table is used to map the devices block number to the backing "block" number which holds the data.

SQLite database doesn't get smaller when data is deleted you need to VACUUM the database to reclaim unused space. This requires up to 3x the disk space of the original database (see https://www.sqlite.org/tempfiles.html#temporary_file_storage_locations for details on changing the location of one of the temporary copies) and may take hours with a large database.
```
sqlite3 /path/to/database.sqlite3 "VACUUM;"
```

# Usage Stats

The following tests are made on an otherwise idle (i7-8700 / NVMe storage / 64GB RAM) server.

A 5TB NBD device with a single partition with an ext4 filesystem was created.

Copying 423 ClusterHAT/ClusterCTRL/Raspbian/Raspberry Pi OS images onto the filesystem used 1.4TB and the backing SQLite database was 128GB (about 10% of stored data).

Write speed with with trustHash enabled was ~100MB/s with trustHash disabled it came down to ~75MB/s.

Reading a single disk image (or all 423 at once) saving to /dev/null with dd the read speed was ~550MB/s.
