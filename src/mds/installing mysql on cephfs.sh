installing mysql on cephfs
# cephfs dir: /home/ceph/cephfs
# mysql datadir: /home/ceph/cephfs/mysql

start mysql with follow question: mysqld_safe Directory '/home/ceph/cephfs/mysql' for UNIX socket file don't exists.
A: directory permission 
# /home/ceph owner: ceph
# /home/ceph/cephfs owner: ceph
# /home/ceph/cephfs/mysql owner: mysql
	$ chmod 777 -R /home/ceph
	$ mysql -u ceph -p 
Then it works.
