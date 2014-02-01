import json

from RedisFileTree import RedisFileTree


class RedisFS:

	def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):

		# Initialize the redis tree client
		self.tree = RedisFileTree(redis_host, redis_port, redis_db)
		self.root = '/'
		self.current = self.root


	# Helper methods
	# ==============

	def _full_path(self, path):
		if path.startswith('/'):
			return path.rstrip('/')

		return '/'. join([self.current, path]).replace('//', '/').rstrip('/')


	def _clear_file_system(self):
		self.tree.delete_tree()


	def _initialize_file_system(self):
		self._clear_file_system()
		self.tree.create_node(self.root, {'type': 'root'})


	def _is_file(self, full_path):
		node = self.tree.read_node(full_path)
		if node:
			data = json.loads(node['data'])
			if data.get('type') == 'file':
				return True
		return False


	def _is_dir(self, full_path):
		if full_path in ['/', '']:
			return True

		node = self.tree.read_node(full_path)
		if node:
			data = json.loads(node['data'])
			if data.get('type') == 'directory':
				return True
		return False


	def _file_exists(self, full_path):
		return (self.tree.read_node(full_path) and True) or False


	def _get_parent(self, full_path):
		return full_path.rsplit('/', 1)[0]


	# File System methods
	# ===================

	def access(self, path, *args, **kwargs):
		path = self._full_path(path)

		if not self._is_file(path):
			raise Exception('61: Not a file')

		return self.tree.read_node(path)['data']


	def ls(self, path=None, *args, **kwargs):
		if not path:
			path = self.current
		else:
			path = self._full_path(path)

		if not self._is_dir(path):
			raise Exception('73: Not a directory')

		dirents = ['.', '..']
		
		for child in self.tree.enumerate_children(path):
			dirents.append(child['name'])

		return dirents


	def chmod(self, path, mode, *args, **kwargs):
		pass

	def chown(self, path, uid, gid, *args, **kwargs):
		pass

	def getattr(self, path, *args, **kwargs):
		pass

	def readlink(self, path, *args, **kwargs):
		pass

	def mkdir(self, path, mode=0666, *args, **kwargs):
		path = self._full_path(path)

		if self._file_exists(path):
			raise Exception('99: File exists')

		loc, name = path.rsplit('/', 1)
		if not self._is_dir(loc):
			raise Exception('103: Not a directory')
		
		self.tree.create_node(path, {'type': 'directory', 'mode': mode})


	def touch(self, path, mode=0666, *args, **kwargs):
		self.write(path, '', mode)


	def rmdir(self, path, *args, **kwargs):
		path = self._full_path(path)

		if not self._is_dir(path):
			raise Exception('116: Not a directory')

		self.tree.delete_node(path)


	def statsfs(self, path, *args, **kwargs):
		pass

	def unlink(self, path, *args, **kwargs):
		pass

	def symlink(self, target, name, *args, **kwargs):
		pass

	def rename(self, old, new, *args, **kwargs):
		pass

	def link(self, target, name, *args, **kwargs):
		pass


	def remove(self, path, *args, **kwargs):
		path = self._full_path(path)

		if not self._is_file(path):
			raise Exception('141: Not a file')

		self.tree.delete_node(path)


	def move(self, path1, path2, *args, **kwargs):
		
		path1 = self._full_path(path1)
		path2 = self._full_path(path2)

		if not self._file_exists(path1):
			raise Exception('152: Does not exist')

		if self._file_exists(path2):
			raise Exception('155: File exists')

		loc, name = path2.rsplit('/', 1)
		if not self._is_dir(loc):
			raise Exception('159: Not a directory')

		self.tree.move_node(path1, path2)


	def cd(self, path=None, *args, **kwargs):
		if path is None or path == '':
			self.current = self.root
		elif path == '.':
			self.current = self.current
		elif self.current != '/' and path == '..':
			self.current = self._get_parent(self.current)
		elif self._file_exists(self._full_path(path)):
			self.current = self._full_path(path)
		else:
			raise Exception('174: Does not exist')


	def pwd(self, *args, **kwargs):
		return self.current


	# File methods
	# ============

	def read(self, path, flags=None):
		f = self.access(path)
		return f['content']
		

	def write(self, path, content, mode=0666):
		path = self._full_path(path)

		if self._file_exists(path):
			raise Exception('File exists')

		loc, name = path.rsplit('/', 1)
		if not self._is_dir(loc):
			raise Exception('Not a directory')

		self.tree.create_node(path, {'type': 'file', 'mode': mode, 'content': content})

