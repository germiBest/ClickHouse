#pragma once

#include <chrono>
#include <mutex>
#include <vector>
#include <absl/container/flat_hash_set.h>
#include <base/types.h>
#include <boost/container_hash/hash_fwd.hpp>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

namespace Coordination
{

class ZooKeeperWatchesTracker
{
public:
    struct Info
    {
        std::string relative_path;
        WatchCallbackPtrOrEventPtr watch;
        std::chrono::system_clock::time_point create_time;
        XID request_xid;
        OpNum op_num;
    };

    using InfoConstPtr = std::shared_ptr<const Info>;
    using WatchesInfoSnapshot = std::vector<InfoConstPtr>;

    ZooKeeperWatchesTracker() = default;

    void add(const std::string & path, const ZooKeeperRequestPtr & request, const WatchCallbackPtrOrEventPtr & watch);
    void removeWatch(const std::string & path, const WatchCallbackPtrOrEventPtr & watch);
    void clear();

    WatchesInfoSnapshot getSnapshot() const;

private:
    mutable std::mutex mutex;

    struct KeyHash
    {
        using is_transparent = void;

        size_t operator()(const InfoConstPtr & key) const
        {
            size_t seed = 0;
            boost::hash_combine(seed, key->relative_path);
            boost::hash_combine(seed, key->watch.hash());
            return seed;
        }

        size_t operator()(const std::pair<std::string_view, WatchCallbackPtrOrEventPtr> & key) const
        {
            size_t seed = 0;
            boost::hash_combine(seed, key.first);
            boost::hash_combine(seed, key.second.hash());
            return seed;
        }
    };

    struct KeyEqual
    {
        using is_transparent = void;

        bool operator()(const InfoConstPtr & lhs, const InfoConstPtr & rhs) const
        {
            return (lhs->relative_path == rhs->relative_path) && (lhs->watch == rhs->watch);
        }

        bool operator()(const InfoConstPtr & lhs, const std::pair<std::string_view, WatchCallbackPtrOrEventPtr> & rhs) const
        {
            return (lhs->relative_path == rhs.first) && (lhs->watch == rhs.second);
        }
    };

    using PathToWatchesMap = absl::flat_hash_set<InfoConstPtr, KeyHash, KeyEqual>;
    PathToWatchesMap watches;
};

}
