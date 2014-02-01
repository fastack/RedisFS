import redis
import hashlib
import json


class RedisFileTree:

	def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
		
		self.pool = redis.ConnectionPool(host=redis_host,
										 port=redis_port,
										 db=redis_db)

		self.r = redis.Redis(connection_pool=self.pool)
		
		# Assert connection to redis server
		self.r.ping()


	# Node methods
	# ============

	def create_node(self, path, attributes):

		uid = path.rstrip('/')

		self.r.hset("NODE:%s" % uid, 'data', json.dumps(attributes))

		while '/' in path:

			path, child = path.rsplit('/', 1)

			p_uid = path
			self.r.hmset("NODE:%s" % uid, {'name': child, 'parent': p_uid})

			self.r.sadd("CHILDREN:%s" % p_uid, uid)
			uid = p_uid

		self.r.hmset("NODE:%s" % uid, {'name': path, 'parent': None})


	def delete_node(self, path):

		uid = path.rstrip('/')

		p_uid = self.r.hget("NODE:%s" % uid, 'parent')
		
		children = self.r.smembers("CHILDREN:%s" % uid)
		for child in children:
			name = self.r.hget("NODE:%s" % child, 'name')
			self.delete_node('/'.join(path, name))

		self.r.delete("CHILDREN:%s" % uid)
		self.r.delete("NODE:%s" % uid)
		self.r.srem("CHILDREN:%s" % p_uid, uid)


	def move_node(self, orig_path, dest_path):

		self.copy_node(orig_path, dest_path)
		self.delete_node(orig_path)


	def copy_node(self, orig_path, dest_path):

		uid = orig_path.rstrip('/')

		content = self.r.hget("NODE:%s" % uid, 'data')

		new_uid = self.create_node(dest_path, content)

		children = self.r.smembers("CHILDREN:%s" % uid)
		for child in children:
			name = self.r.hget("NODE:%s" % child, 'name')
			self.copy_node('/'.join(orig_path, name), 
				           '/'.join(dest_path, name))


	def read_node(self, path):

		uid = path.rstrip('/')
		return self.r.hgetall("NODE:%s" % uid)


	# Tree methods
	# ============

	def delete_tree(self):
		# TODO: Preserve database, only remove tree
		self.r.flushdb()


	def recursively_enumerate_children(self, path):

		uid = path.rstrip('/')

		reply = {"path": uid}

		children = self.r.smembers("CHILDREN:%s" % uid)

		if children:
			reply["children"] = []
			for child in children:
				reply["children"].append(self.recursively_enumerate_children(child))

		reply["self"] = self.read_node(uid)
		yield reply

	def enumerate_children(self, path):

		uid = path.rstrip('/')

		children = self.r.smembers("CHILDREN:%s" % uid)
		for child in children:
			yield self.read_node(child)
