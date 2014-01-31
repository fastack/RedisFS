from redistree import RedisTree


class RedisFS:

	def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):

		# Initialize the redis tree client
		self.r = RedisTree(redis_host, redis_port, redis_db)


	# Helper methods
	# ==============

	def _real_path(self, path):
		pass


	# File System methods
	# ===================

	def access(self, path):
		pass

	def listdir(self, path):
		pass

	def chmod(self, path, mode):
		pass

	def chown(self, path, mode):
		pass

	def getattr(self, path):
		pass

	def readlink(self, path):
		pass

	def mkdir(self, path, mode):
		pass

	def touch(self, path, mode=0600):
		pass

	def rmdir(self, path):
		pass

	def statsfs(self, path):
		pass

	def unlink(self, path):
		pass

	def symlink(self, target, name):
		pass

	def rename(self, old, new):
		pass

	def link(self, target, name):
		pass

	def remove(self, path):
		pass

	def move(self, path):
		pass

	def cd(self, path):
		pass

	def pwd(self):
		pass

	def ls(self):
		pass


	# File methods
	# ============

	def read(self, path, flags=None):
		uid = self.r.get_node_at_path(path)
		return 'uid: %s' % uid

	def write(self, path, data):
		uid = self.r.create_child_node(path)
		return 'write, uid: %s' % uid


fs = RedisFS()

print fs.write('hello/world', {'some': 'data'})
print fs.read('hello/world')




