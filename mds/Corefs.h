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

// corefs
#define SCORE_INC 0.05
#define SCORE_DEC 0.1


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

class C_MDS_inode_update_finish : public MDSInternalContext {
  MDRequestRef mdr;
  CInode *in;
  bool truncating_smaller, changed_ranges;
public:
  C_MDS_inode_update_finish(MDSRank *m, MDRequestRef& r, CInode *i,
			    bool sm=false, bool cr=false) :
    MDSInternalContext(m), mdr(r), in(i), truncating_smaller(sm), changed_ranges(cr) { }
  void finish(int r) {
  	dout(2) << __func__ << dendl;
    assert(r == 0);

    // apply
    in->pop_and_dirty_projected_inode(mdr->ls);
    mdr->apply();

    // notify any clients
    if (truncating_smaller && in->inode.is_truncating()) {
      mds->locker->issue_truncate(in);
      mds->mdcache->truncate_inode(in, mdr->ls);
    }

    mds->balancer->hit_inode(mdr->get_mds_stamp(), in, META_POP_IWR);

    mds->server->respond_to_request(mdr, 0);

    if (changed_ranges)
      mds->locker->share_inode_max_size(in);
  }
};

void Server::corefs_handle_client_feedback(MDRequestRef& mdr){
	dout(2) << __func__ << " got metadata request feedback" << dendl;
	MClientRequest *req = mdr->client_request;
	bufferlist::iterator p = req->get_data().begin();
	uint64_t flush_refer_id, flush_dc_id;
	float flush_score_update;
	int zero;
	void* tmp;
	dout(2) << __func__ << " check-1" << dendl;
	while(p != req->get_data().end()){
		::decode(flush_refer_id, p);
		::decode(flush_dc_id, p);
		// ::decode(flush_score_update, p);
		// ::decode(zero, p);
		// dout(2) << __func__ << " " << flush_refer_id << " | " << flush_dc_id << " | " << flush_score_update << dendl;
		dout(2) << __func__ << " " << flush_refer_id << " --> " << flush_dc_id << dendl;
		// dout(2) << __func__ << " " << zerLq:qasdfo << dendl;

		set<SimpleLock*> rdlocks, wrlocks, xlocks;
		CInode *target = mdcache->get_inode(flush_refer_id);
		// filepath fp;
		// target->make_path(fp);
		// dout(2) << __func__ << " target filepath=" << fp << dendl;
		// mdr->corefs_set_filepath(fp);
		// filepath f = mdr->get_filepath();
		// dout(2) << __func__ << f << dendl;

		CInode *dc = mdcache->get_inode(flush_dc_id);
		filepath fp_dc;
		dc->make_path(fp_dc);
		dout(2) << __func__ << " data correlation.filepath = " << fp_dc << dendl;
		
		// target = rdlock_path_pin_ref(mdr, 0, rdlocks, true);
		// if(!target){
		// 	dout(2) << __func__ << " target CInode is NULL" << dendl;
		// 	continue;
		// }
		// corefs_print_xattrs(target);

		// xlocks.insert(&target->xattrlock);
		// if(!mds->locker->acquire_locks(mdr, rdlocks, wrlocks, xlocks)){
		// 	dout(2) << __func__ << " acquire_locks failed" << dendl;
		// 	continue;
		// }
		
		map<string, bufferptr> *core_px = new map<string, bufferptr>;
		inode_t *pi = target->project_inode(core_px);

		map<string, bufferptr>::iterator iter_key;
		if(!core_px)
			continue;
		iter_key = core_px->find("/"+fp_dc.get_path());
		// iter_key = core_px->find("/" + fp_dc.get_path());
		// get old value
		if(iter_key != core_px->end()){
			// pi->version = target->pre_dirty();
			// pi->ctime = mdr->get_op_stamp();
			// pi->xattr_version++;

			tmp = (void*)malloc(sizeof(dc_value));
			// dout(2) << __func__ << " has dc of path." << fp_dc << dendl;
			memcpy(tmp, iter_key->second.c_str(), sizeof(dc_value));
			dc_value *ds = reinterpret_cast<dc_value*>(tmp);
			dout(2) << __func__ << " path = " << fp_dc << " score = " << ds->score << dendl;
			ds->score -= SCORE_DEC;
			// update score
			// if(flush_score_update)
			// 	ds->score = (ds->score + flush_score_update > 1) ? 1 : (ds->score + flush_score_update);
			// else
			// 	ds->score = (ds->score + flush_score_update < 0) ? 0 : (ds->score + flush_score_update);
			// iter_key->second = buffer::copy((const char*)ds, sizeof(dc_value));
			corefs_print_xattrs(target);
			bufferptr bp;
  			bp = buffer::copy((const char*)ds, sizeof(dc_value));
  			core_px->erase("/"+fp_dc.get_path());
  			core_px->insert(pair<string, bufferptr>("/"+fp_dc.get_path(), bp));
			// iter_key->second.copy_in(0,sizeof(dc_value), reinterpret_cast<char*>(ds));
			corefs_print_xattrs(target);

			// mdr->ls = mdlog->get_current_segment();
			// EUpdate *le = new EUpdate(mdlog, "setxattr");
			// mdlog->start_entry(le);
			// le->metablob.add_client_req(req->get_reqid(), req->get_oldest_client_tid());
			// mdcache->predirty_journal_parents(mdr, &le->metablob, target, 0, PREDIRTY_PRIMARY);
			// mdcache->journal_dirty_inode(mdr.get(), &le->metablob, target);
			// mdr->committing = true;
			// submit_mdlog_entry(le, new C_MDS_inode_update_finish(mds, mdr, target), mdr, __func__);
			// mdlog->flush();

			// // target->project_inode()->xattr_version++;
			// // dout(2) << __func__ << " after update" << dendl;
			// corefs_print_xattrs(target);
			// target->mark_dirty(target->pre_dirty(), mdlog->get_current_segment()); 
			free(tmp);
			// corefs_print_xattrs(target);
		}
		else
			dout(2) << __func__ << " no data correlation of " << fp_dc << dendl;
	}
	dout(2) << __func__ << " done." << dendl;
	mdcache->request_finish(mdr);
	return ;
}

float Server::corefs_get_dc_score(CInode *in, string filename){
	map<string, bufferptr> *core_px = in->get_projected_xattrs();
	map<string, bufferptr>::iterator it = core_px->find(filename);
	if(it == core_px->end())
		return -1;
	float score;
	void *tmp = (void*)malloc(sizeof(dc_value));
	memcpy(tmp, it->second.c_str(), sizeof(dc_value));
	dc_value *ds = reinterpret_cast<dc_value*>(tmp);
	dout(2) << __func__ << " filename." << filename << " score." << ds->score << dendl;
	score = ds->score;
	free(tmp);
	return score;
}

void Server::corefs_print_xattrs(CInode *in){
	map<string, bufferptr>::iterator iter;
  	dout(1) << __func__ << dendl;
  	void *tmp;
  	string s_tmp;
	map<string, bufferptr> *core_px = in->get_projected_xattrs();
  	for(iter=(*core_px).begin(); iter!=(*core_px).end(); iter++){
    	dout(1) << __func__ << " " << iter->first << dendl;
    	tmp = (void*)malloc(sizeof(dc_value));
    	memcpy(tmp, iter->second.c_str(), sizeof(dc_value));
    	dc_value *ds = reinterpret_cast<dc_value *>(tmp);
    	dout(2) << __func__ << " get xattrs <" << iter->first << ", " << ds->score << ", " << ds->offset << ", " << ds->len << "> into inode." << *in << dendl;
    	free(tmp);
    	// dout(2) << __func__ << " set xattrs <" << iter->first << ", " << *((int*)tmp) << ", " << *(long*)((int*)tmp+1) << ", " << *(long*)((int*)tmp+3) << " done." << dendl;
  	}
  	dout(2) << __func__ << " done" << dendl;
	// string s = "corefs.correlation";
	// void *tmp;
	// char *c = NULL;
	// uint64_t len;

	// map<string,bufferptr> *px = in->get_projected_xattrs();
	// if((len = (*px)[s].length()) > 0){
	// 	tmp = (void *)malloc(sizeof(char) * (len + 1));
	// 	memcpy(tmp, (*px)[s].c_str(), len);
	// 	c = (char*)tmp;
	// 	c[len - 1] = 0;
	// }
	// dout(1) << __func__ << " corefs get xattrs:<" << s <<", " << c <<"> from CInode" << dendl;
}

void Server::corefs_add_dc_score(CInode *in, string filename){
	dout(2) << "corefs_add_dc_score"<< dendl;
	map<string, bufferptr> *core_px = in->get_projected_xattrs();
	map<string, bufferptr>::iterator it = core_px->find(filename);
	if(it == core_px->end())
		return;
	void *tmp = (void*)malloc(sizeof(dc_value));
	memcpy(tmp, it->second.c_str(), sizeof(dc_value));
	dc_value *ds = reinterpret_cast<dc_value*>(tmp);
	ds->score += SCORE_INC;
	corefs_print_xattrs(in);
	bufferptr bp;
	bp = buffer::copy((const char*)ds, sizeof(dc_value));
	core_px->erase(filename);
	core_px->insert(pair<string, bufferptr>(filename, bp));
	// (*core_px)[filename].copy_in(0,sizeof(dc_value), reinterpret_cast<char*>(ds));
	corefs_print_xattrs(in);
	return;
}

char* Server::corefs_get_xattrs(CInode *cin){
	char *c = NULL;
	map<string, bufferptr>::iterator p = cin->xattrs.begin();
	c = const_cast<char*>(p->first.c_str());
	// string type_correlation = "corefs.correlation";
	// void *tmp;
	// char* c = NULL;
	// uint64_t len;

	// map<string,bufferptr> *px = cin->get_projected_xattrs();
	// if((len = (*px)[type_correlation].length()) > 0){
	// 	tmp = (void *)malloc(sizeof(char) * (len));
	// 	memcpy(tmp, (*px)[type_correlation].c_str(), len);
	// 	c = (char*)tmp;
	// 	c[len - 1] = 0;
	// }
	
	// dout(1) << __func__ << " corefs get xattrs:<" << type_correlation <<", " << c <<"> from CInode" << dendl;
	return c;
}

int Server::corefs_get_correlations(CInode *target, char** c){
	*c = corefs_get_xattrs(target);
	if(*c == NULL)
		return 0;
	return 1;
}

CInode* Server::corefs_prefetch_cinode(char* filename){
	dout(1) << __func__ << " filename = " << filename << dendl;
	filepath path(filename);
	CInode *cur = mdcache->cache_traverse(path);
	if(cur)
		return cur;
	return NULL;
}

void Server::mon_req_init(){
	int i = 0;
	for(;i<23;i++){
		mon_op[i] = 0;
		mon_req[i] = 0;
	}
	return;
}

void* Server::count_load_thread(void* args){
	int i;
	int load;
	int ops;
	int tmp[OP_NUM] = {0};
	Server* server = reinterpret_cast<Server *>(args);
	MDSRank* mds = server->mds;
	while(1){
		i = 0;
		load = 0;
		ops = 0;
		sleep(1);
		for(;i<OP_NUM;i++){
			ops += (server->mon_op[i] - tmp[i]);
			tmp[i] = server->mon_op[i];
			load += server->mon_req[i];
		}
		dout(1) << "-" << __func__ << "-" << " current_load_ops." << load << dendl;
		dout(1) << "[" << __func__ << "]" << " iops." << ops << dendl;
	}
	return NULL;
}

#endif