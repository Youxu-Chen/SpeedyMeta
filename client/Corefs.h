#ifndef CEPH_COREFS_CLIENT_H
#define CEPH_COREFS_CLIENT_H

#include "include/types.h"

// stl
#include <functional>
#include <string>
#include <memory>
#include <set>
#include <map>
#include <fstream>
#include <exception>
using std::set;
using std::map;
using std::fstream;

#include "include/unordered_set.h"
#include "include/unordered_map.h"

#include "include/filepath.h"
#include "include/interval_set.h"
#include "include/lru.h"

//#include "barrier.h"

#include "mds/mdstypes.h"
#include "mds/MDSMap.h"

#include "msg/Message.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"

#include "common/Mutex.h"
#include "common/Timer.h"
#include "common/Finisher.h"
#include "common/compiler_extensions.h"
#include "common/cmdparse.h"

#include "osdc/ObjectCacher.h"

#include "InodeRef.h"
#include "UserGroups.h"

#include "Client.h"
#include <unistd.h>
#include <sys/types.h>
#include <time.h>
#include <utime.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/utsname.h>
#include <sys/uio.h>
#include <stdlib.h>

#include <boost/lexical_cast.hpp>
#include <boost/fusion/include/std_pair.hpp>

#if defined(__FreeBSD__)
#define XATTR_CREATE    0x1
#define XATTR_REPLACE   0x2
#else
#include <sys/xattr.h>
#endif

#if defined(__linux__)
#include <linux/falloc.h>
#endif

#include <sys/statvfs.h>

#include <iostream>
using namespace std;

#include "common/config.h"

#include "common/version.h"


// ceph stuff

#include "messages/MMonMap.h"

#include "messages/MClientSession.h"
#include "messages/MClientReconnect.h"
#include "messages/MClientRequest.h"
#include "messages/MClientRequestForward.h"
#include "messages/MClientReply.h"
#include "messages/MClientCaps.h"
#include "messages/MClientLease.h"
#include "messages/MClientSnap.h"
#include "messages/MCommandReply.h"
#include "messages/MOSDMap.h"
#include "messages/MClientQuota.h"

#include "messages/MGenericMessage.h"

#include "messages/MMDSMap.h"
#include "messages/MFSMap.h"

#include "mon/MonClient.h"

#include "mds/flock.h"
#include "osd/OSDMap.h"
#include "mon/MonMap.h"

#include "osdc/Filer.h"
#include "osdc/WritebackHandler.h"

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/perf_counters.h"
#include "common/admin_socket.h"
#include "common/errno.h"
#include "include/str_list.h"

#define dout_subsys ceph_subsys_client
#include <regex.h>
#include <regex>

#include "include/lru.h"
#include "include/compat.h"
#include "include/stringify.h"
#include "Client.h"
#include "Inode.h"
#include "Dentry.h"
#include "Dir.h"
#include "ClientSnapRealm.h"
#include "Fh.h"
#include "MetaSession.h"
#include "MetaRequest.h"
#include "ObjecterWriteback.h"
#include "posix_acl.h"

#include "include/assert.h"
#include "include/stat.h"

#if HAVE_GETGROUPLIST
#include <grp.h>
#include <pwd.h>
#include <unistd.h>
#endif

// #undef dout_prefix
// #define dout_prefix *_dout << "client." << whoami << " "

// #define  tout(cct)       if (!cct->_conf->client_trace.empty()) traceout

#define COREFS_SEMANTIC_CODE
// #define COREFS_SRC
// #define COREFS_OVERWRITE
#define COREFS_FEEDBACK
#define PERIOD_PREFETCH_ACCURACY

#define SCORE_INIT 0.5
// #define SCORE_INCREASE 0.05
// #define SCORE_DECREASE -0.05
#define SECOND_FEEDBACK_FLUSH 4

typedef struct dc_v{
  float score;
  size_t len;
  long long offset;
}dc_value;

typedef struct feedback_info{
  utime_t time_phd;
  float accessed;
}feedback;

typedef struct feedback_info_flush{
  uint64_t refer_id;
  uint64_t dc_id;
}feedback_flush;

class FSMap;
class MonClient;

class CephContext;
class MClientReply;
class MClientRequest;
class MClientSession;
class MClientRequest;
class MClientRequestForward;
struct MClientLease;
class MClientCaps;
class MClientCapRelease;

struct DirStat;
struct LeaseStat;
struct InodeStat;

class Filer;
class Objecter;
class WritebackHandler;

class PerfCounters;

void Client::show_table_feedback(){
  ldout(cct, 2) << __func__  << dendl;
  std::unordered_map<uint64_t, std::unordered_map<uint64_t, struct feedback_info*>>::iterator iter = table_feedback.begin();
  for(; iter != table_feedback.end(); iter++){
    std::unordered_map<uint64_t, struct feedback_info*>::iterator it_m = iter->second.begin();
    while(it_m != iter->second.end()){
      // ldout(cct, 2) << __func__ << " " << iter->first << " | " << it_m->first << " | " << it_m->second->time_phd << " | " << it_m->second->score_update << dendl;
      it_m++;
    }
  }
  return;
}

void Client::show_access_sequence(){
  ldout(cct, 2) << __func__  << dendl;
  std::vector<uint64_t>::iterator iter= access_sequence.begin();
  for(; iter != access_sequence.end(); iter++)
    ldout(cct, 2) << __func__ << " " << *iter << dendl;
  return;
}

void Client::corefs_update_feedback(uint64_t ino){
  ldout(cct, 2) << __func__ << " " << ino << dendl;
  int gap = 1;
  // show_access_sequence();
  // show_table_feedback();
  if(access_sequence.size() <= 1)
    return;
  std::vector<uint64_t>::reverse_iterator riter = access_sequence.rbegin();
  std::unordered_map<uint64_t, struct feedback_info*>::iterator it;
  riter++;
  for(; riter != access_sequence.rend() && *riter != ino; riter++, gap++){
    // ldout(cct, 2)  << __func__ << " breakpoint-1 " << *riter << dendl;
    if((it = table_feedback[ino].find(*riter)) != table_feedback[ino].end()){
      ldout(cct, 2) << __func__ << " " << *riter << " score = " << it->second->accessed << dendl;
      // it->second->score_update += SCORE_INCREASE/gap;
      it->second->accessed = 1;
      ldout(cct, 2) << __func__ << " " << *riter << " score_updated = " << it->second->accessed << dendl;
    }
  }

  // it = table_feedback[ino].begin();
  // for(; it != table_feedback[ino].end(); it++)
  //   it->second->accessed = 1;
  return;
}

void Client::corefs_access(uint64_t ino, bool prefetched){
  // ldout(cct, 2) << __func__ << " " << ino << dendl;
  access_sequence.push_back(ino);
  if(prefetched){
    // in is prefetched, so update score in table_feedback
    ldout(cct, 2) << __func__ << " " << ino << " is prefetched, so update score" << dendl;
    // show_access_sequence();
    // show_table_feedback();
    corefs_update_feedback(ino);
    // show_table_feedback();
  }
  return;
}

void Client::mon_op_init(){
  int i = 0;
  for(;i<34;i++)
    mon_op[i] = 0;
  return;
}

void* Client::count_iops_thread(void* args){
  int i;
  unsigned int ops;
  unsigned int tmp[OP_NUM] = {0};
  float total_prefetch_accuracy = 0;
  float period_prefetch_accuracy = 0;
  Client* client = reinterpret_cast<Client *>(args);
  while(1){
    i = 0;
    ops = 0;
    sleep(1);
    for(;i<OP_NUM;i++){
      ops += (client->mon_op[i] - tmp[i]);
      tmp[i] = client->mon_op[i];
    }
    ldout(client->Dispatcher::cct, 1) << "[" << __func__ << "]" << " iops." << ops << dendl;
  }
  return NULL;
}

void Client::corefs_send_feedback(MetaRequest* request){
  request->set_tid(++last_tid);
  request->op_stamp = ceph_clock_now(NULL);
  request->resend_mds = rand() % mdsmap->get_num_in_mds();
  // get a random mds
  while(1){
    mds_rank_t mds;
    std::set<mds_rank_t> up;
    mdsmap->get_up_mds_set(up);
    if(up.empty())
      mds = -1;
    std::set<mds_rank_t>::const_iterator p = up.begin();
    for(int n = rand() % up.size(); n; n--)
      ++p;
    mds = *p;
    ldout(cct, 2) << __func__ << " mds = " << mds << dendl;
    if(mds < MDS_RANK_NONE || !mdsmap->is_active_or_stopping(mds)){
      ldout(cct, 2) << __func__ << " target mds." << mds << " not active, waiting for new mdsmap" << dendl;
      wait_on_list(waiting_for_mdsmap);
      continue;
    }

    MetaSession *session = NULL;
    if(!have_open_session(mds)){
      if(!mdsmap->is_active_or_stopping(mds)){
        ldout(cct, 2) << __func__ << " no address for mds." << mds << ", waiting for new mdsmap" << dendl;
        wait_on_list(waiting_for_mdsmap);
      }
      if(!mdsmap->is_active_or_stopping(mds)){
        ldout(cct, 2) << __func__ << " hmm, still have no address for mds." << mds << ", trying a random mds" << dendl;
        p = up.begin();
        for(int n = rand() % up.size(); n; n--)
          ++p;
        request->resend_mds = *p;
        continue;
      }
      session = _get_or_open_mds_session(mds);
      if(session->state == MetaSession::STATE_OPENING){
        ldout(cct, 2) << __func__ << " waiting for session to mds." << mds << " to open" << dendl;
        wait_on_context_list(session->waiting_for_open);
        if(rejected_by_mds.count(mds)){
          ldout(cct, 2) << __func__ << " rejected by mds." << mds << dendl;
          request->abort(-EPERM);
          break;
        }
        continue;
      }

      if(!have_open_session(mds))
        continue;
    }
    else
      session = mds_sessions[mds];
    // if(!have_open_session(mds))
    //   continue;
    // else
    //   session = mds_sessions[mds];
    // ldout(cct, 2) << __func__ << " send request to mds." << mds << dendl;
    send_request(request, session);
    ldout(cct, 2) << __func__ << " send request to mds." << mds << dendl;
    break;
  }
  return;
}

void* Client::adaptive_feedback(void* args){
  Client* client = reinterpret_cast<Client*>(args);
  // ldout(client->Dispatcher::cct, 1) << args << dendl;
  ldout(client->Dispatcher::cct, 1) << __func__ << dendl;
  bufferlist bl;
  // filepath fp;
  // Inode *i_tmp;
  // bool set_fp = false;
  int flush = 1;
  float total_prefetch_accuracy = 0;
  float period_prefetch_accuracy = 0;
  uint64_t traffic_feedback = 0;
  while(1){
    bl.clear();
    sleep(SECOND_FEEDBACK_FLUSH);
    traffic_feedback = 0;
    // if(!client->mounted)
    //   break;
    #ifdef COREFS_FEEDBACK 
    std::unordered_map<uint64_t, std::unordered_map<uint64_t, struct feedback_info*>>::iterator iter = client->table_feedback.begin();
    // ldout(client->Dispatcher::cct, 1) << __func__ << " check-3 feedback table" << dendl;
    for(; iter != client->table_feedback.end(); iter++){
      // ldout(client->Dispatcher::cct, 1) << __func__ << " check " << iter->first << dendl;
      std::unordered_map<uint64_t, struct feedback_info*>::iterator it_m = iter->second.begin();
      for(; it_m != iter->second.end(); ){
        // ldout(client->Dispatcher::cct, 1) << __func__ << " check-4 " << iter->first << " --> " << it_m->first << dendl;
        // check whether this item should be flushed
        if(ceph_clock_now(client->Dispatcher::cct) - it_m->second->time_phd >= SECOND_FEEDBACK_FLUSH){
          if(it_m->second->accessed == 0){
            feedback_flush* item = new feedback_flush();
            item->refer_id = it_m->first;
            item->dc_id = iter->first;
            // ldout(client->Dispatcher::cct, 1) << __func__ << " check-6 feedback table" << dendl;
            ldout(client->Dispatcher::cct, 2) << __func__ << " " << item->refer_id << " --> " << item->dc_id << dendl;
            bl.append(reinterpret_cast<const char*>(item), sizeof(feedback_flush));
            traffic_feedback += sizeof(feedback_flush);
            free(it_m->second);
            delete item;
          }
          it_m = iter->second.erase(it_m);
          ldout(client->Dispatcher::cct, 1) << __func__ << " check-12 feedback table" << dendl;
        }
        else
          it_m++;
      }
    }
    if(bl.c_str() != NULL){
      ldout(client->Dispatcher::cct, 1) << "[adaptive_feedback] network-traffic-feedback=" << traffic_feedback << dendl;
      // build a metadata request to flush feedback to MDS
      MetaRequest *request = new MetaRequest(CEPH_MDS_OP_FEEDBACK);
      request->op_stamp = ceph_clock_now(NULL);
      // request->set_filepath(fp);
      // request->set_inode(i_tmp);
      // ldout(client->Dispatcher::cct, 1) << "[adaptive_feedback] length of data is " << bl.length() << dendl;
      request->set_data(bl);
      // client->make_request(request, -1, -1, NULL, NULL, rand() % client->mdsmap->get_num_in_mds() );
      client->corefs_send_feedback(request);
    }
    #endif
    if(flush % 2 == 0){
      #ifdef PERIOD_PREFETCH_ACCURACY
      // client->access_sequence.clear(); // clear access sequence every SECOND_FEEDBACK_FLUSH seconds
      total_prefetch_accuracy = client->total_phd_access/client->total_phd;
      ldout(client->Dispatcher::cct, 1) << "[total-prefetch-accuracy] " << client->total_phd_access << "/" << client->total_phd << " " << total_prefetch_accuracy << dendl;
      // clear false positive and re-compute in this T seconds
      period_prefetch_accuracy = client->period_phd_access/client->period_phd;
      ldout(client->Dispatcher::cct, 1) << "[period-prefetch-accuracy] " << client->period_phd_access << "/" << client->period_phd << " " << period_prefetch_accuracy << dendl;
      client->period_phd = 0;
      client->period_phd_access = 0;
      // client->inode_times_phd.clear();
      client->access_sequence.clear();
      ceph::unordered_map<vinodeno_t, Inode*>::iterator it = client->inode_map.begin();
      for(; it != client->inode_map.end(); it++)
          it->second->period_times_phd = 0;
      #endif
    }
    flush++;
  }
  client->table_feedback.clear();
  return NULL;
}

void Client::corefs_print_xattrs(Inode *in){
  void *tmp;
  string s_tmp;
  // string s_name = "1";
  // uint64_t size;
  
  map<string, bufferptr>::iterator iter;
  ldout(cct, 2) << __func__ << dendl;

  for(iter=in->xattrs.begin(); iter!=in->xattrs.end(); iter++){
    ldout(cct, 1) << __func__ << " " << iter->first << dendl;
    tmp = (void*)malloc(sizeof(dc_value));
    memcpy(tmp, iter->second.c_str(), sizeof(dc_value));

    dc_value *ds = reinterpret_cast<dc_value *>(tmp);
    ldout(cct, 2) << __func__ << " get xattrs <" << iter->first << ", " << ds->score << ", " << ds->offset << ", " << ds->len << "> into inode." << *in << dendl;
    free(tmp);
    // ldout(cct, 2) << __func__ << " set xattrs <" << iter->first << ", " << *((int*)tmp) << ", " << *(long*)((int*)tmp+1) << ", " << *(long*)((int*)tmp+3) << "> into ino " << in->ino << " done." << dendl;
  }
}

void Client::corefs_set_xattrs(Inode *in, string key, uint64_t size, void* value){
  // string s="corefs.correlation";
  string s_key = key;
  ldout(cct, 2) << __func__ << " key = " << s_key << dendl;
  // itoa(++size_xattrs, key.c_str(), 10);
  // bufferptr bp;
  bufferptr bp;
  bp = buffer::copy((const char*)value, sizeof(dc_value));
  in->xattrs.insert(pair<string, bufferptr>(s_key, bp));
  in->xattr_version++;
  // corefs_print_xattrs(in);

  dc_value *ds = reinterpret_cast<dc_value *>(value);
  ldout(cct, 2) << __func__ << " set xattrs <" << key << ", " << ds->score << ", " << ds->offset << ", " << ds->len << "> into inode." << *in << dendl;
  return;
  // ldout(cct, 1) << __func__ << " set xattrs <" << key << ", " << *((int*)value) << ", " << *(long*)((int*)value+1) << ", " << *(long*)((int*)value+3) << "> into ino " << in->ino << " done." << dendl;
}


int Client::correlation_overlap(long offset1, int len1, long offset2, int len2){
  if((offset1 >= offset2 && offset1 <= offset2 + len2) || (offset2 >= offset1 && offset2 <= offset1 + len1))
    return 1;
  return 0;
}

void Client::corefs_extract_correlations(Inode *in, const char *buf, long offset){
  const char* pattern;
  int start;
  #ifdef COREFS_SEMANTIC_CODE
    pattern = "^#include\\s(\"|<)((\\w+(/|_|-)?)+)\\.h(\"|>)$";
    start = 10;
  #endif

  #ifdef COREFS_SRC
    pattern = "^src=/fileset/(\\w+)$";
    start = 4;
  #endif

  int coursor_char;
  int num_slash;
  int dep_path;
  regex_t reg;
  regcomp(&reg, pattern, REG_EXTENDED | REG_NEWLINE);
  // regcomp(&reg, pattern, REG_EXTENDED);
  const size_t nmatch = 1;
  regmatch_t pmatch[1];
  int flag_match = 0;
  char* data = (char *)buf;
  // ldout(cct, 2) << "[corefs_prefetch]" << dendl;
  filepath path_in_long;
  if(!in)
    return;
  in->make_long_path(path_in_long);
  ldout(cct, 2) << "long path = " << path_in_long << dendl;

  char value[20];
  int score = 1;
  long corre_offset = 0;
  long corre_len;
  long pass = 0;
  string s_corre;

  while(!regexec(&reg, data, nmatch, pmatch, 0)){
    //Matched!
    flag_match = 1;
    ldout(cct, 2) << " pattern matched" << dendl;
    ldout(cct, 2) << " start=" << pmatch[0].rm_so <<", end=" << pmatch[0].rm_eo << dendl;
    #ifdef COREFS_SEMANTIC_CODE
    // decrease 11 to exclude other un-correlated chars
    char *correlation = (char *)malloc(sizeof(char)*(pmatch[0].rm_eo - pmatch[0].rm_so - start));
    strncpy(correlation, &data[pmatch[0].rm_so + start], pmatch[0].rm_eo - pmatch[0].rm_so - start);
    correlation[pmatch[0].rm_eo - pmatch[0].rm_so - start - 1] = '\0';
    ldout(cct, 2) << "corefs.correlation = " << correlation << dendl;
    
    coursor_char = 0;
    num_slash = 0;
    // while(correlation[coursor_char]!='\0'){
    //   if(correlation[coursor_char] == '/')
    //     num_slash++;
    //   coursor_char++;
    // }
    // ldout(cct, 2) << "number of slash =  " << num_slash << dendl;
    filepath new_corre;
    if(data[pmatch[0].rm_so + 9] == '<'){
      // similar as format:#include <linux/fs.h>
      // add prefix: /linux-4.14.2/include/
      ldout(cct, 2) << "linux-include" << dendl;
      new_corre.set_path("/linux-4.14.2/include");
      new_corre.push_dentry(correlation);
    }
    else{
      ldout(cct, 2) << "client-include" << dendl;
      dep_path = path_in_long.depth();
      ldout(cct, 2) << "depth = " << dep_path << dendl;
      new_corre = path_in_long.prefixpath(dep_path- num_slash - 1);
      ldout(cct, 2) << "number of prefix in path = " << new_corre << dendl;
      new_corre.push_dentry(correlation);
    }
    
    ldout(cct, 2) << "new correlation = " << new_corre << dendl;
    ldout(cct, 2) << "new correlation_2 = " << "/"+new_corre.get_path() << dendl;

    s_corre = "/" + new_corre.get_path();
    ldout(cct, 2) << "final correlation = " << s_corre << dendl;
    #endif

    #ifdef COREFS_SRC
    char *correlation = (char *)malloc(sizeof(char)*(pmatch[0].rm_eo - pmatch[0].rm_so - start));
    strncpy(correlation, &data[pmatch[0].rm_so + start], pmatch[0].rm_eo - pmatch[0].rm_so - start + 1);
    correlation[pmatch[0].rm_eo - pmatch[0].rm_so - start] = '\0';
    ldout(cct, 2) << "corefs.correlation = " << correlation << dendl;
    #endif

    dc_value *ds = (dc_value*)malloc(sizeof(dc_value));
    ds->score = SCORE_INIT;
    ds->offset = pmatch[0].rm_so + start + pass + offset;
    ds->len = pmatch[0].rm_eo - pmatch[0].rm_so - start - 1;

    // corre_offset = pmatch[0].rm_so + start + pass;
    // corre_len = pmatch[0].rm_eo - pmatch[0].rm_so - start - 1;
    // ldout(cct, 2) << "offset=" << corre_offset << ", len=" << corre_len << dendl;
    // strncpy(value, (char*)&score, sizeof(int));
    // strncpy(value+sizeof(int), (char*)&corre_offset, sizeof(long));
    // strncpy(value+sizeof(int)+sizeof(long), (char*)&corre_len, sizeof(long));

    #ifdef COREFS_SRC
    s_corre = correlation;
    #endif

    // is there overwritten?
    #ifdef COREFS_OVERWRITE
    map<string, bufferptr>::iterator iter;
    void *tmp;
    for(iter = in->xattrs.begin(); iter != in->xattrs.end(); ){
      tmp = (void*)malloc(sizeof(dc_value));
      memcpy(tmp, iter->second.c_str(), sizeof(dc_value));
      dc_value *tmp_dc = reinterpret_cast<dc_value *>(tmp);
      float tmp_offset = tmp_dc->offset;
      float tmp_len = tmp_dc->len;
      ldout(cct, 2) << __func__ << " get xattrs <" << iter->first << ", " << tmp_dc->score << ", " << tmp_offset << ", " << tmp_len << "> into inode." << *in << dendl;
      if (tmp_offset + tmp_len <= in->size){
        if(correlation_overlap(tmp_offset, tmp_len, ds->offset, ds->len)){
          // overlapped with existing correlations iter and remove iter
          ldout(cct, 2) << __func__ << " overlapped with " << iter->first << " and remove " << iter->first << dendl;
          in->xattrs.erase(iter++);
        }
        else
          iter++;
      }
      else{
        ldout(cct, 2) << __func__ << iter->first << " is beyond of existing file size and should remove it."  << dendl;
        in->xattrs.erase(iter++);
      }
    }

    free(tmp);
    #endif

    // insert new correlations
    corefs_set_xattrs(in, s_corre, s_corre.size(), reinterpret_cast<void *>(ds));
    // corefs_set_xattrs(in, s_corre.c_str(), s_corre.size(), value);

    free(correlation);
    free(ds);
    pass = pmatch[0].rm_eo;
    data += pass;
    if(!*data)
      break;
  }
  if(flag_match)
    mark_caps_dirty(in, CEPH_CAP_XATTR_EXCL);
  else
    ldout(cct, 2) << "pattern not matched" << dendl;
  ldout(cct, 2) << " semantic analysis done." << dendl;
  regfree(&reg);
}

#endif
