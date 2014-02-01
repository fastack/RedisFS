from RedisFS import RedisFS


if __name__ == "__main__":
	fs = RedisFS()
	fs._initialize_file_system()  ## Deletes all data and starts with fresh database

	while(True):

		args = raw_input("RedisFileSystem:~%s$ " % fs.current).split(' ')

		if args[0] == 'exit':
			raise SystemExit

		try:

			command = getattr(fs, args[0])
			resp = command(*args[1:])
			if resp:
				if isinstance(resp, list):
					resp = ' '.join(resp)
				print resp

		except Exception as e:
			print "Error: %s" % str(e)
		