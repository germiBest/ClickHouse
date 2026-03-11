#include <chrono>
#include <mutex>
#include <base/defines.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperWatchesTracker.h>


namespace Coordination
{

void ZooKeeperWatchesTracker::removeWatch(const std::string & path, const WatchCallbackPtrOrEventPtr & watch)
{
    std::lock_guard lock(mutex);
    watches.erase(std::make_pair(path, watch));
}

void ZooKeeperWatchesTracker::clear()
{
    std::lock_guard lock(mutex);
    watches.clear();
}

ZooKeeperWatchesTracker::WatchesInfoSnapshot ZooKeeperWatchesTracker::getSnapshot() const
{
    std::lock_guard lock(mutex);

    WatchesInfoSnapshot result;
    result.reserve(watches.size());

    for (const auto & info : watches)
        result.emplace_back(info);

    return result;
}

void ZooKeeperWatchesTracker::add(const std::string & path, const ZooKeeperRequestPtr & request, const WatchCallbackPtrOrEventPtr & watch)
{
    chassert(watch);

    auto info = std::make_shared<Info>();
    info->relative_path = path;
    info->watch = watch;
    info->request_xid = request->xid;
    info->op_num = request->getOpNum();
    info->create_time = std::chrono::system_clock::now();

    std::lock_guard lock(mutex);
    watches.emplace(std::move(info));
}
}
