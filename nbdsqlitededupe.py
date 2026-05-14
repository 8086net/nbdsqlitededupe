#
# NBD SQLite DeDupe
# https://github.com/8086net/nbdsqlitededupe
# (c) Chris Burton 2023-2026
#
# On Debian install the following
# apt install python3 nbdkit nbdkit-plugin-python nbd-client sqlite3 python3-zstandard python3-lz4
#
# To start server (remove "-f" to background process, remove "-v" for hide debug output)
# nbdkit -i localhost -p 10810 -v -f python nbdsqlitededupe.py db=nbdsqlitededupe.sqlite3 size=1T compress=yes 2>&1 | tee -a nbdsqlitededupe.log
#
# size = number of bytes for the device
# db = database filename
# compress = yes/zstd/lz4/lzma/zlib/no (optional, defaults to no)
# unsafewrite = yes/no (optional, defaults to no. Disables fsync for faster bulk loading)
# trusthash = yes/no (optional, defaults to no)
#
# To start client side
# modprobe nbd max_part=8
# Create NBD device (must use 4k block)
# nbd-client -b 4096 localhost 10810 /dev/nbd0
#
# v1.0 2023-09-20 Initial version
# v1.1 2024-02-21 Removed compression
# v1.2 2024-02-24 Keep track of block usage to allow cleanup
# v1.3 2024-02-26 Add size parameter
# v1.4 2024-03-02 Add compression back
# v1.5 2024-03-03 Don't write all zero blocks
# v1.6 2025-08-07 Use transactions/retries
# v1.7 2026-05-13 Add extents + general tidy
# v1.8 2026-05-14 Auto upgrade database for compression support
# v1.9 2026-05-14 Add queue to pool db connections, move compress/hash code outside of db transaction
# v2.0 2026-05-14 Multi-method compression support (Zstd, LZ4, LZMA, zlib), add unsafewrite flag for bulk loading performance
# v2.1 2026-05-14 Expose trusthash as an option to make for easier enabling during bulk loading
#

import nbdkit
import errno
import sqlite3
import os
import hashlib
import time
import zlib
import queue

# Optional compression libraries
try:
    import zstandard as zstd
except ImportError:
    zstd = None

try:
    import lz4.block as lz4_block
except ImportError:
    lz4_block = None

try:
    import lzma
except ImportError:
    lzma = None

# 4k blocks (Can't be changed without creating a new database)
blocksize = 4096

# Trust sha256 hash of blocks are unique
# Trusting is faster but hash collisions will cause data loss
trustHash = False

#
# End of config
#

RETRY_SLEEP = 0.1
API_VERSION = 2

filename = None
db = None
blocks = None
zero_chunk = None
compress = 0 # 0=none, 1=zstd, 2=lz4, 3=lzma, 4=zlib
minshrink = 128
unsafewrite = False

db_pool = queue.Queue()

def config(key, value):
    global filename, blocksize, blocks, compress, unsafewrite, trustHash
    if key == "db":
        filename = os.path.abspath(value)
    elif key == "size":
        if value.isnumeric():
            blocks = int(value)
            if blocks % blocksize:
                blocks = blocks + blocksize
            blocks = (int)(blocks/blocksize)
        else:
            try:
                blocks = nbdkit.parse_size(value)
                if blocks % blocksize:
                    blocks = blocks + blocksize
                blocks = (int)(blocks/blocksize)
            except:
                raise RuntimeError("nbdkit.parse_size missing size must be specified in bytes")
    elif key == "compress":
        if value == "yes" or value == "zstd":
            if zstd is None:
                raise RuntimeError("zstd support requested but python3-zstandard is not installed")
            compress = 1
        elif value == "lz4":
            if lz4_block is None:
                raise RuntimeError("lz4 support requested but python3-lz4 is not installed")
            compress = 2
        elif value == "lzma":
            if lzma is None:
                raise RuntimeError("lzma support requested but lzma is not available")
            compress = 3
        elif value == "zlib":
            compress = 4
        elif value == "no":
            compress = 0
        else:
            nbdkit.debug("ignored compress value %s" % value)
    elif key == "unsafewrite" and value == "yes":
        unsafewrite = True
    elif key == "trusthash" and value == "yes":
        trustHash = True
    else:
        nbdkit.debug("ignored parameter %s=%s" % (key, value))


def config_complete():
    global filename

    if filename is None:
        raise RuntimeError("file parameter is required")
    if blocks is None:
        raise RuntimeError("size parameter is required")


def get_db():
    try:
        return db_pool.get_nowait()
    except queue.Empty:
        db = sqlite3.connect(filename, timeout=60.0, check_same_thread=False)
        db.execute("PRAGMA journal_mode=WAL")
        if unsafewrite:
            db.execute("PRAGMA synchronous=OFF")
        else:
            db.execute("PRAGMA synchronous=NORMAL")
        db.execute("PRAGMA cache_size=-64000")
        return db

def return_db(db):
    db_pool.put(db)


def open(readonly):
    global zero_chunk, blocksize

    db = sqlite3.connect(filename, timeout=60.0)
    db.execute("PRAGMA journal_mode=WAL")
    if unsafewrite:
        db.execute("PRAGMA synchronous=OFF")
    else:
        db.execute("PRAGMA synchronous=NORMAL")

    c = db.cursor()

    try:
        c.execute("CREATE TABLE block (id INTEGER PRIMARY KEY, hash BLOB, data BLOB, cnt INTEGER, c INTEGER)")
        c.execute("CREATE TABLE mapper (id INTEGER PRIMARY KEY, block_id INTEGER)")
        c.execute("CREATE INDEX bh ON block(hash)")
        c.execute("CREATE INDEX bc ON block(cnt)")
        c.execute("CREATE INDEX mb ON mapper(block_id)")
    except:
        pass

    # Try an auto upgrade for old databases without compression
    try:
        c.execute("ALTER TABLE block ADD COLUMN c INTEGER")
    except:
        pass

    db.commit()
    c.close()

    db.close()

    zero_chunk = bytes(blocksize)

    return 1


def get_size(h):
    global blocksize, blocks
    return blocksize*blocks


def pread(h, buf, offset, flags):
    global blocksize

    if len(buf) % blocksize:
        raise RuntimeError("length of buffer not divisible")

    if offset % blocksize:
        raise RuntimeError("offset not divisible")

    # Zero buffer
    l = len(buf)
    buf[0:l] = bytearray(l)

    startblock = int(offset/blocksize)
    nblocks = int(len(buf)/blocksize)

    db = get_db()
    try:
        c = db.cursor()

        done = False
        while not done:
            try:
                for b in c.execute("SELECT mapper.id,block.data,block.c FROM block JOIN mapper ON mapper.block_id=block.id WHERE mapper.id>=? AND mapper.id<?", (startblock, startblock+nblocks,) ):
                    o = (b[0]-startblock)*blocksize

                    if b[2]==1: # zstd
                        buf[o:(o+blocksize)] = zstd.ZstdDecompressor().decompress(b[1], max_output_size=blocksize)
                    elif b[2]==2: # lz4
                        buf[o:(o+blocksize)] = lz4_block.decompress(b[1], uncompressed_size=blocksize)
                    elif b[2]==3: # lzma
                        buf[o:(o+blocksize)] = lzma.decompress(b[1])
                    elif b[2]==4: # zlib
                        buf[o:(o+blocksize)] = zlib.decompress(b[1])
                    else:
                        buf[o:(o+blocksize)] = b[1]
                done = True
            except sqlite3.OperationalError as e:
                if str(e) == 'database is locked':
                    nbdkit.debug("locked retrying")
                    time.sleep(RETRY_SLEEP)
                else:
                    raise e
        c.close()
    finally:
        return_db(db)


def pwrite(h, buf, offset, flags):
    global blocksize, filename, trustHash, zero_chunk, compress, minshrink

    if len(buf) % blocksize:
        raise RuntimeError("length of buffer not divisible")
    if offset % blocksize:
        raise RuntimeError("offset not divisible")

    startblock = int(offset/blocksize)
    nblocks = int(len(buf)/blocksize)

    # Multi-core pre-processing (outside DB lock)
    processed_blocks = []
    query = []

    for n in range(nblocks):
        chunk = buf[blocksize*n:(blocksize*(n+1))]

        if chunk == zero_chunk:
            processed_blocks.append(None) # Mark as an zero block
        else:
            tmp = hashlib.sha256(chunk).digest()
            store_data = chunk
            is_compressed = 0

            if compress == 1: # zstd
                zl = zstd.ZstdCompressor(level=3).compress(chunk)
                if (len(zl) + minshrink) < blocksize:
                    store_data = zl
                    is_compressed = 1
            elif compress == 2: # lz4
                zl = lz4_block.compress(chunk, store_size=False)
                if (len(zl) + minshrink) < blocksize:
                    store_data = zl
                    is_compressed = 2
            elif compress == 3: # lzma
                zl = lzma.compress(chunk)
                if (len(zl) + minshrink) < blocksize:
                    store_data = zl
                    is_compressed = 3
            elif compress == 4: # zlib
                zl = zlib.compress(chunk)
                if (len(zl) + minshrink) < blocksize:
                    store_data = zl
                    is_compressed = 4

            processed_blocks.append((tmp, store_data, is_compressed))
            if trustHash:
                query.append(tmp)

    db = get_db()
    try:
        c = db.cursor()

        done = False
        while not done:
            try:
                c.execute("BEGIN IMMEDIATE")

                if trustHash:
                    # Find blocks with matching hash (if we have non-zero blocks to query)
                    blocks = []
                    if query:
                        blocks = c.execute("SELECT id,hash FROM block WHERE hash IN (%s)" % ','.join('?'*len(query)), query ).fetchall()

                    # Loop blocks being written
                    for n in range(nblocks):
                        pb = processed_blocks[n]

                        if pb is None: # Zero block - remove any existing data
                            block_id = c.execute("SELECT block_id FROM mapper WHERE id=? LIMIT 1", (startblock+n, ) ).fetchone()
                            if block_id:
                                c.execute("UPDATE block SET cnt=cnt-1 WHERE id=?", (block_id[0], ) )
                                c.execute("DELETE FROM mapper WHERE id=?", (startblock+n, ) )
                            continue # Skip the rest of the loop for this block

                        tmp, store_data, is_compressed = pb
                        found = False
                        # Loop existing blocks
                        for b in blocks:
                            if not found and b[1] == tmp:
                                block_id = c.execute("SELECT block_id FROM mapper WHERE id=? LIMIT 1", (startblock+n, ) ).fetchone()
                                if block_id and block_id[0] == b[0]: # new/old block_id are the same
                                    pass
                                elif block_id: # mapper exists but different block_id
                                    c.execute("UPDATE block SET cnt=cnt-1 WHERE id=?", (block_id[0], ) ) # decrement usage on old block
                                    c.execute("UPDATE mapper SET block_id=? WHERE id=?", ( b[0], startblock+n, ) )
                                    c.execute("UPDATE block SET cnt=cnt+1 WHERE id=?", (b[0], ) ) # increment usage on new block
                                else: # mapper doesn't exist
                                    c.execute("INSERT INTO mapper VALUES (?, ?)", (startblock+n, b[0], ) )
                                    c.execute("UPDATE block SET cnt=cnt+1 WHERE id=?", (b[0], ) ) # increment usage on new block

                                found = True

                        if not found:
                            # Block doesn't exist so insert it
                            c.execute("INSERT INTO block (hash,data,cnt,c) VALUES (?, ?, 1, ?)", (tmp, store_data, is_compressed,) )
                            id = c.lastrowid
                            c.execute("INSERT INTO mapper VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET block_id=?", (startblock+n, id, id, ) )
                            blocks.append( (id, tmp,) ) # Add block to list of blocks so it isn't added again in this pwrite call
                else: # hash not trusted
                    for n in range(nblocks):
                        pb = processed_blocks[n]

                        if pb is None: # Zero block - remove any existing data
                            block_id = c.execute("SELECT block_id FROM mapper WHERE id=? LIMIT 1", (startblock+n, ) ).fetchone()
                            if block_id:
                                c.execute("UPDATE block SET cnt=cnt-1 WHERE id=?", (block_id[0], ) )
                                c.execute("DELETE FROM mapper WHERE id=?", (startblock+n, ) )
                            continue # Skip the rest of the loop for this block

                        # Generate hash for data blocks being written
                        h, store_data, is_compressed = pb

                        # Find block where hash and data match
                        b = c.execute("SELECT id,cnt FROM block WHERE hash=? AND data=? AND c=? LIMIT 1", (h, store_data, is_compressed,) ).fetchone()

                        if b:
                            block_id = c.execute("SELECT block_id FROM mapper WHERE id=? LIMIT 1", (startblock+n, ) ).fetchone()
                            if block_id and block_id[0] == b[0]: # new/old block_id are the same
                                pass
                            elif block_id: # mapper exists but different block_id
                                c.execute("UPDATE block SET cnt=cnt-1 WHERE id=?", (block_id[0], ) ) # decrement usage on old block
                                c.execute("UPDATE mapper SET block_id=? WHERE id=?", ( b[0], startblock+n, ) )
                                c.execute("UPDATE block SET cnt=cnt+1 WHERE id=?", (b[0], ) ) # increment usage on new block
                            else: # mapper doesn't exist
                                c.execute("INSERT INTO mapper VALUES (?, ?)", (startblock+n, b[0], ) )
                                c.execute("UPDATE block SET cnt=cnt+1 WHERE id=?", (b[0], ) ) # increment usage on new block

                        else: # block not found
                            c.execute("INSERT INTO block (hash,data,cnt,c) VALUES (?, ?, 1, ?)", (h, store_data, is_compressed,) )
                            id = c.lastrowid
                            c.execute("INSERT INTO mapper VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET block_id=?", (startblock+n, id, id, ) )

                # Tidy up any blocks no longer in use
                c.execute("DELETE FROM block WHERE cnt<=0")

                db.commit()
                done = True

            except sqlite3.OperationalError as e:
                db.rollback()
                if str(e) == 'database is locked':
                    nbdkit.debug("locked retrying")
                    time.sleep(RETRY_SLEEP)
                else:
                    raise e
            except Exception as e:
                db.rollback()
                raise e

        c.close()
    finally:
        return_db(db)


def block_size(h):
    global blocksize

    return (blocksize, blocksize*64, blocksize*8192) # 4kB/ 256kB / 32MB with 4kB blocks


def trim(h, count, offset, flags):
    global blocksize

    if count % blocksize:
        raise RuntimeError("count not divisible")
    if offset % blocksize:
        raise RuntimeError("offset not divisible")

    startblock = int(offset/blocksize)
    nblocks = int(count/blocksize)

    db = get_db()
    try:
        c = db.cursor()

        # Get number of times each block we're removing is used and decrement the block usage cnt
        done = False
        while not done:
            try:
                # Start transaction
                c.execute("BEGIN IMMEDIATE")

                # Get counts of blocks being removed
                c.execute("SELECT count(id), block_id FROM mapper WHERE id>=? AND id<? GROUP BY block_id", (startblock, startblock+nblocks,))
                blocks_to_decrement = c.fetchall()

                # Decrement usage count
                for b in blocks_to_decrement:
                    c.execute("UPDATE block SET cnt=cnt-? WHERE id=?", (b[0], b[1],) )

                # Remove unused mappers
                c.execute("DELETE FROM mapper WHERE id>=? AND id<?", (startblock, startblock+nblocks,) )

                # Remove any unused blocks
                c.execute("DELETE FROM block WHERE cnt<=0")

                db.commit()
                done = True

            except sqlite3.OperationalError as e:
                db.rollback()
                if str(e) == 'database is locked':
                    nbdkit.debug("locked retrying")
                    time.sleep(RETRY_SLEEP)
                else:
                    raise e
            except Exception as e:
                db.rollback()
                raise e

        c.close()
    finally:
        return_db(db)


def zero(h, count, offset, flags):
    trim(h, count, offset, flags)

def thread_model():
    return nbdkit.THREAD_MODEL_PARALLEL

# Allow multiple connections
def can_multi_conn(h):
    return True

# We use trim to zero so it should be fast
def can_fast_zero(h):
    return True

## Extents - returns list of hole/data blocks

def can_extents(h):
    return True

def extents(h, count, offset, flags):
    global blocksize

    if count % blocksize:
        raise RuntimeError("count not divisible")
    if offset % blocksize:
        raise RuntimeError("offset not divisible")

    startblock = int(offset/blocksize)
    nblocks = int(count/blocksize)
    endblock = startblock + nblocks

    db = get_db()
    try:
        c = db.cursor()

        done = False
        while not done:
            try:
                c.execute("SELECT id FROM mapper WHERE id>=? AND id<?", (startblock, endblock,))

                # Get mapper ids
                mapped_ids = set([row[0] for row in c.fetchall()])
                done = True
            except sqlite3.OperationalError as e:
                if str(e) == 'database is locked':
                    nbdkit.debug("locked retrying")
                    time.sleep(RETRY_SLEEP)
                else:
                    raise e

        c.close()
    finally:
        return_db(db)

    extents_list = []
    current_type = None
    current_start = startblock

    # Loop through blocks and group them by type
    for b in range(startblock, endblock):
        if b in mapped_ids: # Block exists
            block_type = 0 # Allocated data
        else: # Block missing
            block_type = nbdkit.EXTENT_HOLE | nbdkit.EXTENT_ZERO

        # First extent setup
        if current_type is None:
            current_type = block_type
            current_start = b

        # When type changes add previous run of blocks to list
        elif current_type != block_type:
            length = (b - current_start) * blocksize
            extents_list.append( (current_start * blocksize, length, current_type) )

            # Setup new extent
            current_start = b
            current_type = block_type

    # Add last extent
    if current_type is not None:
        length = (endblock - current_start) * blocksize
        extents_list.append( (current_start * blocksize, length, current_type) )

    return extents_list
