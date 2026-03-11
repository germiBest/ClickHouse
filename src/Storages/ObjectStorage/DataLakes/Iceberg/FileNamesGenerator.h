#pragma once

#include <IO/CompressionMethod.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

#include <Poco/UUIDGenerator.h>

namespace DB
{

#if USE_AVRO

class FileNamesGenerator
{
public:
    struct Result
    {
        /// Path recorded in the Iceberg metadata files.
        /// If `write_full_path_in_iceberg_metadata` is disabled, it will be a simple relative path (e.g., /a/b/c.avro).
        /// Otherwise, it will include a prefix indicating the file system type (e.g., s3://a/b/c.avro).
        String path_in_metadata;

        /// Actual path to the object in the storage (e.g., /a/b/c.avro).
        String path_in_storage;
    };

    FileNamesGenerator() = default;
    explicit FileNamesGenerator(
        const String & table_dir_,
        const String & storage_dir_,
        bool use_uuid_in_metadata_,
        CompressionMethod compression_method_,
        const String & format_name_);

    FileNamesGenerator(const FileNamesGenerator & other);
    FileNamesGenerator & operator=(const FileNamesGenerator & other);

    Result generateDataFileName();
    Result generateManifestEntryName();
    Result generateManifestListName(Int64 snapshot_id, Int32 format_version);
    Result generateMetadataName();
    Result generateVersionHint();
    Result generatePositionDeleteFile();

    String convertMetadataPathToStoragePath(const String & metadata_path) const;

    void setVersion(Int32 initial_version_) { initial_version = initial_version_; }
    void setCompressionMethod(CompressionMethod compression_method_) { compression_method = compression_method_; }

private:
    Poco::UUIDGenerator uuid_generator;
    String table_dir;
    String storage_dir;

    String data_dir;
    String metadata_dir;
    String storage_data_dir;
    String storage_metadata_dir;
    bool use_uuid_in_metadata;
    CompressionMethod compression_method;
    String format_name;

    Int32 initial_version = 0;
};

/// Returns {config_path, storage_path} for FileNamesGenerator construction.
/// config_path: path for Iceberg metadata (starts with '/').
/// storage_path: path for actual object storage operations (no leading '/').
inline std::pair<String, String> getConfigAndStoragePaths(const String & table_path)
{
    auto config_path = table_path;
    if (config_path.empty() || config_path.back() != '/')
        config_path += "/";
    if (!config_path.starts_with('/'))
        config_path = '/' + config_path;

    auto storage_path = table_path;
    if (storage_path.empty() || storage_path.back() != '/')
        storage_path += "/";

    return {config_path, storage_path};
}

/// Returns the directory prefix for paths written into Iceberg metadata files.
/// When write_full_path is true, uses table_location (e.g. wasb://container@host/path/)
/// so that other engines like Spark can resolve the paths.
/// Otherwise, uses config_path (e.g. /path/) for local/relative paths.
inline String getMetadataDir(const String & table_path, const String & table_location, bool write_full_path)
{
    if (write_full_path)
    {
        auto bucket = table_location;
        if (bucket.empty() || bucket.back() != '/')
            bucket += "/";
        return bucket;
    }
    auto [config_path, _] = getConfigAndStoragePaths(table_path);
    return config_path;
}

#endif

}
