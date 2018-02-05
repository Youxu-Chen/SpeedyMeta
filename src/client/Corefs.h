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


void Client::corefs_print_xattrs(Inode *in){
  void *tmp;
  string s_tmp;
  string s_name = "corefs.correlation";
  uint64_t len;

  ldout(cct, 10) << __func__ << dendl;
  if((len = in->xattrs[s_name].length()) > 0){
    tmp = (void *)malloc(sizeof(char) * (len + 1));
    memcpy(tmp, in->xattrs[s_name].c_str(), len);
    s_tmp = (char *)tmp;
    s_tmp = s_tmp + "\0";
    ldout(cct, 7) << "Cor-FS: get xattrs <" << s_name << ", " << s_tmp << "> from ino " << in->ino << dendl;
  }
}

void Client::corefs_set_xattrs(Inode *in, const char *buf, uint64_t size){
  string s="corefs.correlation";
  bufferptr bp;

  if(size > 0){
    bp = buffer::copy(buf,size);
    in->xattrs.insert(pair<string,bufferptr>(s,bp));
    ldout(cct, 10) << __func__ << " start setxattr" << dendl;
    in->xattr_version++;
    // _do_setxattr(in,s.c_str(),buf,size,CEPH_XATTR_CREATE,in->uid,in->gid);
  }

  ldout(cct, 10) << __func__ << " set xattrs <" << s << ", " << buf << "> into ino " << in->ino << " done." << dendl;
}

#endif