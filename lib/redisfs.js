/* redisfs.js */

function RedisFS(host, port, db){
	host = typeof host !== 'undefined' ? host : "localhost";
	port = typeof port !== 'undefined' ? port : 6379;
	db   = typeof   db !== 'undefined' ?   db : 0;

}