#
# NBD SQLite DeDupe
# https://github.com/8086net/nbdsqlitededupe
# (c) Chris Burton 2023-2024
#
# On Debian install the following
# apt install python3 nbdkit nbdkit-plugin-python nbd-client sqlite3
#
# To start server (remove "-f" to background process, remove "-v" for hide debug output)
# nbdkit -i localhost -p 10810 -v -f python nbdsqlitededupe.py db=nbdsqlitededupe.sqlite3 size=1T 2>&1 | tee -a nbdsqlitededupe.log
#
# size = number of bytes for the device
# db = database filename
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
#

import nbdkit
import errno
import sqlite3
import os
import hashlib
import time

# 4k blocks (Can't be changed without creating a new database)
blocksize =  4096

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

def config(key, value):
	global filename, blocksize, blocks
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
	else:
		nbdkit.debug("ignored parameter %s=%s" % (key, value))


def config_complete():
	global filename

	if filename is None:
		raise RuntimeError("file parameter is required")
	if blocks is None:
		raise RuntimeError("size parameter is required")


def open(readonly):
	global db
	db = sqlite3.connect(filename)
	
	c = sqlite_retry_cursor(db)
	try:
		c.execute("CREATE TABLE block (id INTEGER PRIMARY KEY, hash BLOB, data BLOB, cnt INTEGER)")
		c.execute("CREATE TABLE mapper (id INTEGER PRIMARY KEY, block_id INTEGER)")
		c.execute("CREATE INDEX bh ON block(hash)")
		c.execute("CREATE INDEX bc ON block(cnt)")
		c.execute("CREATE INDEX mb ON mapper(block_id)")
	except:
		pass

	sqlite_retry_close(db, c)
	return 1


def get_size(h):
	global blocksize, blocks
	return blocksize*blocks


def pread(h, buf, offset, flags):
	global blocks, blocksize, filename, db

	if len(buf) % blocksize:
		raise RuntimeError("length of buffer not divisible")

	if offset % blocksize:
		raise RuntimeError("offset not divisible")

	startblock = int(offset/blocksize)
	nblocks = int(len(buf)/blocksize)

	c = sqlite_retry_cursor(db)

	done = False
	while not done:
		try:
			for b in c.execute("SELECT mapper.id,block.data FROM block JOIN mapper ON mapper.block_id=block.id WHERE mapper.id>=? AND mapper.id<?", (startblock, startblock+nblocks,) ):
				o = (b[0]-startblock)*blocksize
				buf[o:(o+blocksize)] = b[1]
			done = True
		except sqlite3.OperationalError as e:
			if str(e) == 'database is locked':
				nbdkit.debug("locked retrying")
				time.sleep(RETRY_SLEEP)
			else:
	
				raise e
	sqlite_retry_close(db, c)


def pwrite(h, buf, offset, flags):
	global blocks, blocksize, filename, db, trustHash

	if len(buf) % blocksize:
		raise RuntimeError("length of buffer not divisible")
	if offset % blocksize:
		raise RuntimeError("offset not divisible")

	startblock = int(offset/blocksize)
	nblocks = int(len(buf)/blocksize)

	needTidy = False

	c = sqlite_retry_cursor(db)

	if trustHash:
		# Calculate hashes
		hashes = {}
		query = []
		for n in range(nblocks):
			tmp = hashlib.sha256(buf[blocksize*n:(blocksize*(n+1))]).digest()
			hashes[n] = tmp
			query.append(tmp)

		# Find blocks with matching hash
		blocks = sqlite_retry_fetchall(c, "SELECT id,hash FROM block WHERE hash IN (%s)" % ','.join('?'*len(query)), query )

		# Loop blocks being written
		for n in range(nblocks):
			found = False
			# Loop existing blocks
			for b in blocks:
				if not found and b[1] == hashes[n]:
					block_id = sqlite_retry_fetchone(c, "SELECT block_id FROM mapper WHERE id=? LIMIT 1", (startblock+n, ) )
					if block_id and block_id == b[0]: # new/old block_id are the same
						pass
					elif block_id: # mapper exists but different block_id
						sqlite_retry(c, "UPDATE block SET cnt=cnt-1 WHERE id=?", (block_id[0], ) ) # decrement usage on old block
						sqlite_retry(c, "UPDATE mapper SET block_id=? WHERE id=?", ( b[0], startblock+n, ) )
						sqlite_retry(c, "UPDATE block SET cnt=cnt+1 WHERE id=?", (b[0], ) ) # increment usage on new block
						if b[1]==1: # block usage cnt was 1 so it's now 0
							needTidy = True
					else: # mapper doesn't exist
						sqlite_retry(c, "INSERT INTO mapper VALUES (?, ?)", (startblock+n, b[0], ) )
						sqlite_retry(c, "UPDATE block SET cnt=cnt+1 WHERE id=?", (b[0], ) ) # increment usage on new block

					found = True
	
			if not found:
				# Block doesn't exist so insert it
				sqlite_retry(c, "INSERT INTO block (hash,data,cnt) VALUES (?, ?, 1)", (hashes[n], buf[blocksize*n:(blocksize*(n+1))],) )
				id = c.lastrowid
				sqlite_retry(c, "INSERT INTO mapper VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET block_id=?", (startblock+n, id, id, ) )
				blocks.append( (id, hashes[n],) ) # Add block to list of blocks so it isn't added again in this pwrite call
	else: # hash not trusted
		for n in range(nblocks):
			# Generate hash for data blocks being written
			h = hashlib.sha256(buf[blocksize*n:(blocksize*(n+1))]).digest()
	
			# Find block where hash and data match
			b = sqlite_retry_fetchone(c, "SELECT id,cnt FROM block WHERE hash=? AND data=? LIMIT 1", (h, buf[blocksize*n:(blocksize*(n+1))],) )

			if b:
				block_id = sqlite_retry_fetchone(c, "SELECT block_id FROM mapper WHERE id=? LIMIT 1", (startblock+n, ) )
				if block_id and block_id[0] == b[0]: # new/old block_id are the same
					pass
				elif block_id: # mapper exists but different block_id
					sqlite_retry(c, "UPDATE block SET cnt=cnt-1 WHERE id=?", (block_id[0], ) ) # decrement usage on old block
					sqlite_retry(c, "UPDATE mapper SET block_id=? WHERE id=?", ( b[0], startblock+n, ) )
					sqlite_retry(c, "UPDATE block SET cnt=cnt+1 WHERE id=?", (b[0], ) ) # increment usage on new block
					if b[1]==1: # block usage cnt was 1 so it's now 0
						needTidy = True
				else: # mapper doesn't exist
					sqlite_retry(c, "INSERT INTO mapper VALUES (?, ?)", (startblock+n, b[0], ) )
					sqlite_retry(c, "UPDATE block SET cnt=cnt+1 WHERE id=?", (b[0], ) ) # increment usage on new block
					
			else: # block not found
				sqlite_retry(c, "INSERT INTO block (hash,data,cnt) VALUES (?, ?,1)", (h, buf[blocksize*n:(blocksize*(n+1))],) )
				id = c.lastrowid
				sqlite_retry(c, "INSERT INTO mapper VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET block_id=?", (startblock+n, id, id, ) )
				
	# end if trustHash:

	# Tidy up any blocks no longer in use
	if needTidy:
		sqlite_retry(c, "DELETE FROM block WHERE cnt=0")

	sqlite_retry_close(db, c)


def block_size(h):
	return (1024,1024,1024)


def trim(h, count, offset, flags):
	global filename, blocksize, db

	if count % blocksize:
		raise RuntimeError("count not divisible")
	if offset % blocksize:
		raise RuntimeError("offset not divisible")

	startblock = int(offset/blocksize)
	nblocks = int(count/blocksize)

	c = sqlite_retry_cursor(db)

	# Get number of times each block we're removing is used and decrement the block usage cnt
	done = False
	while not done:
		try:
			for b in c.execute("SELECT count(id),block_id FROM mapper WHERE id>=? AND id<?", (startblock, startblock+nblocks,) ):
				c.execute("UPDATE block SET cnt=cnt-? WHERE id=?", (b[0], b[1],) )
			done = True
		except sqlite3.OperationalError as e:
			if str(e) == 'database is locked':
				nbdkit.debug("locked retrying")
				time.sleep(RETRY_SLEEP)
			else:
				raise e

	# Remove unused mapper
	sqlite_retry(c, "DELETE FROM mapper WHERE id>=? AND id<?", (startblock, startblock+nblocks,) )

	# Remove any unused blocks
	sqlite_retry(c, "DELETE FROM block WHERE cnt=0")

	sqlite_retry_close(db, c)


def zero(h, count, offset, flags):
	trim(h, count, offset, flags)


def sqlite_retry_cursor(db):
	c = db.cursor()
	c.execute("PRAGMA journal_mode=WAL2")
	c.execute("PRAGMA synchronous=off")
	return c


def sqlite_retry_close(db, c):
	done = False
	while not done:
		try:
			db.commit()
			done = True
		except sqlite3.OperationalError as e:
			if str(e) == 'database is locked':
				nbdkit.debug("locked retrying")
				time.sleep(RETRY_SLEEP)
			else:
				raise e
	done = False
	while not done:
		try:
			c.close()
			done = True
		except sqlite3.OperationalError as e:
			if str(e) == 'database is locked':
				nbdkit.debug("locked retrying")
				time.sleep(RETRY_SLEEP)
			else:
				raise e

def sqlite_retry_fetchall(c, query, param=()):
	done = False
	while not done:
		try:
			return c.execute(query, param).fetchall()
		except sqlite3.OperationalError as e:
			if str(e) == 'database is locked':
				nbdkit.debug("locked retrying")
				time.sleep(RETRY_SLEEP)
			else:
				raise e

def sqlite_retry_fetchone(c, query, param=()):
	done = False
	while not done:
		try:
			return c.execute(query, param).fetchone()
		except sqlite3.OperationalError as e:
			if str(e) == 'database is locked':
				nbdkit.debug("locked retrying")
				time.sleep(RETRY_SLEEP)
			else:
				raise e

def sqlite_retry(c, query, param=()):
	done = False
	while not done:
		try:
			c.execute(query, param)
			done = True
		except sqlite3.OperationalError as e:
			if str(e) == 'database is locked':
				nbdkit.debug("locked retrying")
				time.sleep(RETRY_SLEEP)
			else:
				raise e

