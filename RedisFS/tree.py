import redis
import hashlib


class RedisTree:

	def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):

		# By definition, the root node is always the one with the smallest
		# value.
		self.ROOT_NODE = -9223372036854775807
		
		self.pool = redis.ConnectionPool(host=redis_host,
										 port=redis_port,
										 db=redis_db)

		self.r = redis.Redis(connection_pool=self.pool)
		
		# Assert connection to redis server
		self.r.ping()

		self.r.flushdb()


	# Helper methods
	# ==============

	def _uid_from_string(self, key):
		return hashlib.sha1(key).hexdigest()


	# Node methods
	# ============

	def create_node(self, path, attributes):

		uid = self._uid_from_string(path)
		self.r.hset("NODE:%s" % uid, 'data', attributes)

		while '/' in path:

			path, child = path.rsplit('/', 1)

			p_uid = self._uid_from_string(path)
			self.r.hmset("NODE:%s" % uid, {'name': child, 'parent': p_uid})

			self.r.sadd("CHILDREN:%s" % p_uid, uid)
			uid = p_uid

		self.r.hmset("NODE:%s" % uid, {'name': child, 'parent': None})


	def delete_node(self, path):

		uid = self._uid_from_string(path)
		self.r.delete("NODE:%s", % uid)

		children = self.r.smembers("CHILDREN:%s" % uid)
		for child in children:
			name = self.r.hget("NODE:%s" % child, 'name')
			self.delete_node('/'.join(path, name))
		self.r.delete("CHILDREN:%s" % uid)


	def move_node(self, orig_path, dest_path):

		self.copy_node(orig_path, dest_path)
		self.delete_node(orig_path)


	def copy_node(self, orig_path, dest_path):

		uid = self._uid_from_string(orig_path)
		content = self.r.hget("NODE:%s" % uid, 'data')

		new_uid = self.create_node(dest_path, content)

		children = self.r.smembers("CHILDREN:%s" % uid)
		for child in children:
			name = self.r.hget("NODE:%s" % child, 'name')
			self.copy_node('/'.join(orig_path, name), 
				           '/'.join(dest_path, name))


	def read_node(self, path):

		uid = self._uid_from_string(path)
		return self.r.hgetall("NODE:%s" % uid)




fs = RedisTree()

fs.create_node("/path/to/some/file", {"meta": "data"})
fs.read("/path/to/some/file")
fs.move_node("/path/to/some/file", "/path/to/another")
fs.read("/path/to/another")
