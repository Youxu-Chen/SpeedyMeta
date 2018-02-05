#ifndef CEPH_COREFS_MDS_SERVER_H
#define CEPH_COREFS_MDS_SERVER_H

#include "MDSRank.h"
#include "Server.h"

#include <boost/lexical_cast.hpp>
#include "include/assert.h"  // lexical_cast includes system assert.h

#include <boost/config/warning_disable.hpp>
#include <boost/fusion/include/std_pair.hpp>

#include "MDSRank.h"
#include "Server.h"
#include "Locker.h"
#include "MDCache.h"
#include "MDLog.h"
#include "Migrator.h"
#include "MDBalancer.h"
#include "InoTable.h"
#include "SnapClient.h"
#include "Mutation.h"

#include "msg/Messenger.h"

#include "osdc/Objecter.h"

#include "messages/MClientSession.h"
#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"
#include "messages/MClientReconnect.h"
#include "messages/MClientCaps.h"
#include "messages/MClientSnap.h"

#include "messages/MMDSSlaveRequest.h"

#include "messages/MLock.h"

#include "events/EUpdate.h"
#include "events/ESlaveUpdate.h"
#include "events/ESession.h"
#include "events/EOpen.h"
#include "events/ECommitted.h"

#include "include/filepath.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/perf_counters.h"
#include "include/compat.h"
#include "osd/OSDMap.h"

#include <errno.h>
#include <fcntl.h>

#include <list>
#include <iostream>
using namespace std;

#include "common/config.h"
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".server "

class OSDMap;
class PerfCounters;
class LogEvent;
class EMetaBlob;
class EUpdate;
class MMDSSlaveRequest;
struct SnapInfo;
class MClientRequest;
class MClientReply;
class MDLog;

struct MutationImpl;
struct MDRequestImpl;
typedef ceph::shared_ptr<MutationImpl> MutationRef;
typedef ceph::shared_ptr<MDRequestImpl> MDRequestRef;

void Server::corefs_print_xattrs(CInode *in){
	string s = "corefs.correlation";
	void *tmp;
	char *c;
	uint64_t len;

	map<string,bufferptr> *px = in->get_projected_xattrs();
	if((len = (*px)[s].length()) > 0){
		tmp = (void *)malloc(sizeof(char) * (len + 1));
		memcpy(tmp, (*px)[s].c_str(), len);
		c = (char*)tmp;
		c[len - 1] = 0;
	}
	dout(10) << __func__ << " corefs get xattrs:<" << s <<", " << c <<"> from CInode" << dendl;
}

char* Server::corefs_get_xattrs(CInode *cin){
	string type_correlation = "corefs.correlation";
	void *tmp;
	char* c;
	uint64_t len;

	map<string,bufferptr> *px = cin->get_projected_xattrs();
	if((len = (*px)[type_correlation].length()) > 0){
		tmp = (void *)malloc(sizeof(char) * (len));
		memcpy(tmp, (*px)[type_correlation].c_str(), len);
		c = (char*)tmp;
		c[len - 1] = 0;
	}
	
	dout(10) << __func__ << " corefs get xattrs:<" << type_correlation <<", " << c <<"> from CInode" << dendl;
	return c;
}

int Server::corefs_get_correlations(CInode *target, char** c){
	*c = corefs_get_xattrs(target);
	if(*c == NULL)
		return 0;
	return 1;
}

CInode* Server::corefs_prefetch_cinode(char* filename){
	dout(7) << __func__ << " filename = " << filename << dendl;
	filepath path(filename);
	CInode *cur = mdcache->cache_traverse(path);
	if(cur)
		return cur;
	return NULL;
}

void Server::corefs_set_trace_dist_prefetched_cinode(Session *session, MClientReply *reply,
			    CInode *pre_cin, 
			    snapid_t snapid,
			    MDRequestRef& mdr){
	bufferlist bl;
  	// prefetched inode
  	if (pre_cin) {
		pre_cin->encode_inodestat(bl, session, NULL, snapid, 0, mdr->getattr_caps);
		dout(20) << "set_trace_dist added in   " << *pre_cin << dendl;
		reply->head.is_target = 2;//corefs 2:prefetched inode
  } else
    	reply->head.is_target = 0;
}

#endif