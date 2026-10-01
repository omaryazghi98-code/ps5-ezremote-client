#include "direct_downloader.h"

#include <atomic>
#include <fcntl.h>
#include <fstream>
#include <json-c/json.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "clients/baseclient.h"
#include "common.h"
#include "fs.h"
#include "util.h"

namespace DirectDownloader
{
    static const uint64_t kSegmentSize = 16ULL * 1024ULL * 1024ULL;
    static const int kWorkerCount = 8;
    static const int kMaxRetries = 5;
    static const char kResumeMagic[] = "EZRANGE1";

    struct ResumeHeader
    {
        char magic[8];
        uint64_t total_size;
        uint64_t segment_size;
        uint32_t segment_count;
    };

    struct GlobalState
    {
        std::atomic<bool> running{false};
        std::atomic<bool> cancel{false};
        std::atomic<uint64_t> transferred{0};
        std::atomic<uint64_t> total{0};
        std::atomic<uint64_t> segments_done{0};
        std::atomic<uint64_t> segments_total{0};
        std::mutex mutex;
        std::string url;
        std::string path;
        std::string error;
        DownloadState state = STATE_PENDING;
        time_t timestamp = 0;
    };

    static GlobalState g_state;
    static std::atomic<size_t> g_next_segment{0};

    struct WorkerArgs
    {
        int fd;
        std::string host;
        std::string path;
        uint64_t total_size;
        uint64_t segment_size;
        size_t segment_count;
        std::vector<uint8_t> *done;
        std::mutex *resume_mutex;
    };

    struct RunArgs
    {
        std::string url;
        std::string destination;
    };

    static std::string GetFilenameFromUrl(const std::string &url)
    {
        std::string clean = url;
        size_t q = clean.find('?');
        if (q != std::string::npos)
            clean.resize(q);

        size_t hash = clean.find('#');
        if (hash != std::string::npos)
            clean.resize(hash);

        size_t slash = clean.find_last_of('/');
        std::string filename = (slash == std::string::npos) ? clean : clean.substr(slash + 1);
        filename = BaseClient::UnEscape(filename);

        if (filename.empty())
            filename = "download.bin";

        return filename;
    }

    static void SplitUrl(const std::string &url, std::string *host, std::string *path)
    {
        size_t scheme_pos = url.find("://");
        if (scheme_pos == std::string::npos)
            throw std::runtime_error("URL must include http:// or https://");

        size_t root_pos = url.find("/", scheme_pos + 3);
        if (root_pos == std::string::npos)
        {
            *host = url;
            *path = "/";
        }
        else
        {
            *host = url.substr(0, root_pos);
            *path = url.substr(root_pos);
        }
    }

    static std::string NormalizeDestination(const std::string &requested, const std::string &url)
    {
        std::string destination = requested;
        if (destination.empty())
            destination = "/data/homebrew/";

        if (destination.back() == '/' || FS::FolderExists(destination))
        {
            if (destination.back() != '/')
                destination += "/";
            destination += GetFilenameFromUrl(url);
        }

        return destination;
    }

    static std::string ParentDirectory(const std::string &path)
    {
        size_t slash = path.find_last_of('/');
        if (slash == std::string::npos)
            return "/data/homebrew";
        if (slash == 0)
            return "/";
        return path.substr(0, slash);
    }

    static bool LoadResume(const std::string &resume_path,
                           uint64_t total_size,
                           uint64_t segment_size,
                           size_t segment_count,
                           std::vector<uint8_t> *done)
    {
        done->assign(segment_count, 0);

        std::ifstream in(resume_path.c_str(), std::ios::binary);
        if (!in)
            return false;

        ResumeHeader header{};
        in.read(reinterpret_cast<char*>(&header), sizeof(header));
        if (!in ||
            memcmp(header.magic, kResumeMagic, sizeof(header.magic)) != 0 ||
            header.total_size != total_size ||
            header.segment_size != segment_size ||
            header.segment_count != segment_count)
        {
            return false;
        }

        if (done->empty())
            return true;

        in.read(reinterpret_cast<char*>(done->data()), done->size());
        return static_cast<size_t>(in.gcount()) == done->size();
    }

    static bool SaveResume(const std::string &resume_path,
                           uint64_t total_size,
                           uint64_t segment_size,
                           const std::vector<uint8_t> &done)
    {
        ResumeHeader header{};
        memcpy(header.magic, kResumeMagic, sizeof(header.magic));
        header.total_size = total_size;
        header.segment_size = segment_size;
        header.segment_count = static_cast<uint32_t>(done.size());

        std::string tmp = resume_path + ".tmp";
        std::ofstream out(tmp.c_str(), std::ios::binary | std::ios::trunc);
        if (!out)
            return false;

        out.write(reinterpret_cast<const char*>(&header), sizeof(header));
        if (!done.empty())
            out.write(reinterpret_cast<const char*>(done.data()), done.size());
        out.flush();
        if (!out)
            return false;
        out.close();

        remove(resume_path.c_str());
        return rename(tmp.c_str(), resume_path.c_str()) == 0;
    }

    static void SetState(DownloadState state, const std::string &error = std::string())
    {
        std::lock_guard<std::mutex> lock(g_state.mutex);
        g_state.state = state;
        g_state.error = error;
        g_state.timestamp = time(nullptr);
    }

    static bool IsFailed()
    {
        std::lock_guard<std::mutex> lock(g_state.mutex);
        return g_state.state == STATE_FAILED;
    }

    static void MarkFailed(const std::string &message)
    {
        SetState(STATE_FAILED, message);
        g_state.cancel.store(true);
        Util::RichNotify(Util::GetTick(), "Native download failed: %s", message.c_str());
    }

    static void *WorkerMain(void *argp)
    {
        WorkerArgs *args = reinterpret_cast<WorkerArgs *>(argp);

        while (!g_state.cancel.load())
        {
            size_t index = g_next_segment.fetch_add(1);
            if (index >= args->segment_count)
                break;

            {
                std::lock_guard<std::mutex> lock(*args->resume_mutex);
                if ((*args->done)[index])
                    continue;
            }

            uint64_t offset = static_cast<uint64_t>(index) * args->segment_size;
            uint64_t size = std::min<uint64_t>(args->segment_size, args->total_size - offset);

            bool success = false;
            std::string last_error;

            for (int attempt = 0; attempt < kMaxRetries && !g_state.cancel.load(); ++attempt)
            {
                BaseClient client;
                client.Connect(args->host, "", "");

                if (client.GetRangeToFile(args->path, args->fd, size, offset) == 1)
                {
                    success = true;
                    break;
                }

                last_error = client.LastResponse();
                if (attempt + 1 < kMaxRetries)
                    sleep(1 << attempt);
            }

            if (!success)
            {
                MarkFailed("segment " + std::to_string(index) + ": " +
                           (last_error.empty() ? "range request failed" : last_error));
                break;
            }

            {
                std::lock_guard<std::mutex> lock(*args->resume_mutex);
                (*args->done)[index] = 1;
                SaveResume(g_state.path + ".ezresume",
                           args->total_size, args->segment_size, *args->done);
            }

            g_state.transferred.fetch_add(size);
            g_state.segments_done.fetch_add(1);
        }

        delete args;
        return nullptr;
    }

    static void *DownloadThread(void *argp)
    {
        RunArgs *run = reinterpret_cast<RunArgs *>(argp);

        std::string host;
        std::string path;
        std::string destination = NormalizeDestination(run->destination, run->url);
        std::string part_path = destination + ".ezpart";
        std::string resume_path = destination + ".ezresume";
        std::mutex resume_mutex;

        {
            std::lock_guard<std::mutex> lock(g_state.mutex);
            g_state.url = run->url;
            g_state.path = destination;
            g_state.error.clear();
            g_state.timestamp = time(nullptr);
        }

        try
        {
            SplitUrl(run->url, &host, &path);
        }
        catch (const std::exception &e)
        {
            MarkFailed(e.what());
            g_state.running.store(false);
            delete run;
            return nullptr;
        }

        BaseClient probe;
        probe.Connect(host, "", "");

        uint64_t total_size = 0;
        if (!probe.Size(path, &total_size))
        {
            MarkFailed(std::string("could not determine file size: ") + probe.LastResponse());
            g_state.running.store(false);
            delete run;
            return nullptr;
        }

        g_state.total.store(total_size);

        std::string parent = ParentDirectory(part_path);
        FS::MkDirs(parent);

        int fd = open(part_path.c_str(), O_CREAT | O_RDWR, 0666);
        if (fd < 0)
        {
            MarkFailed("cannot open " + part_path + ": " + strerror(errno));
            g_state.running.store(false);
            delete run;
            return nullptr;
        }

        const size_t segment_count = total_size == 0
            ? 1
            : static_cast<size_t>((total_size + kSegmentSize - 1) / kSegmentSize);

        struct stat st{};
        bool valid_part = stat(part_path.c_str(), &st) == 0 &&
                          static_cast<uint64_t>(st.st_size) == total_size;

        if (!valid_part && ftruncate(fd, static_cast<off_t>(total_size)) != 0)
        {
            MarkFailed("cannot allocate destination: " + std::string(strerror(errno)));
            close(fd);
            g_state.running.store(false);
            delete run;
            return nullptr;
        }

        std::vector<uint8_t> done;
        bool resumed = valid_part &&
                       LoadResume(resume_path, total_size, kSegmentSize, segment_count, &done);

        if (!resumed)
        {
            done.assign(segment_count, 0);
            remove(resume_path.c_str());
        }

        uint64_t completed = 0;
        size_t completed_segments = 0;
        for (size_t i = 0; i < segment_count; ++i)
        {
            if (!done[i])
                continue;

            uint64_t offset = static_cast<uint64_t>(i) * kSegmentSize;
            uint64_t size = std::min<uint64_t>(kSegmentSize, total_size - offset);
            completed += size;
            completed_segments++;
        }

        g_state.transferred.store(completed);
        g_state.segments_done.store(completed_segments);
        g_state.segments_total.store(segment_count);
        g_state.cancel.store(false);

        if (total_size == 0 || completed_segments == segment_count)
        {
            fsync(fd);
            close(fd);
            remove(resume_path.c_str());
            remove(destination.c_str());

            if (rename(part_path.c_str(), destination.c_str()) != 0)
                MarkFailed("rename failed: " + std::string(strerror(errno)));
            else
            {
                g_state.transferred.store(total_size);
                g_state.segments_done.store(segment_count);
                SetState(STATE_SUCCESS);
                Util::RichNotify(Util::GetTick(), "Native download complete: %s", destination.c_str());
            }

            g_state.running.store(false);
            delete run;
            return nullptr;
        }

        size_t probe_index = 0;
        while (probe_index < segment_count && done[probe_index])
            probe_index++;

        uint64_t probe_offset = static_cast<uint64_t>(probe_index) * kSegmentSize;
        uint64_t probe_size = std::min<uint64_t>(kSegmentSize, total_size - probe_offset);

        bool ranges_supported = false;
        std::string probe_error;

        for (int attempt = 0; attempt < 3 && !ranges_supported; ++attempt)
        {
            BaseClient range_probe;
            range_probe.Connect(host, "", "");

            if (range_probe.GetRangeToFile(path, fd, probe_size, probe_offset) == 1)
            {
                ranges_supported = true;
                done[probe_index] = 1;
                completed += probe_size;
                completed_segments++;
                g_state.transferred.store(completed);
                g_state.segments_done.store(completed_segments);
                SaveResume(resume_path, total_size, kSegmentSize, done);
            }
            else
            {
                probe_error = range_probe.LastResponse();
                if (attempt < 2)
                    sleep(1 << attempt);
            }
        }

        if (!ranges_supported)
        {
            // Only use the non-range fallback on a fresh download. A partial
            // segmented download cannot be safely resumed without byte ranges.
            if (completed != 0)
            {
                MarkFailed("server did not honor HTTP Range; partial download cannot be resumed" +
                           (probe_error.empty() ? std::string() : ": " + probe_error));
                close(fd);
                g_state.running.store(false);
                delete run;
                return nullptr;
            }

            SetState(STATE_DOWNLOADING);

            BaseClient full;
            full.Connect(host, "", "");
            if (full.Get(part_path, path) != 1)
            {
                MarkFailed(std::string("full download failed: ") + full.LastResponse());
                close(fd);
                g_state.running.store(false);
                delete run;
                return nullptr;
            }

            fsync(fd);
            close(fd);
            remove(resume_path.c_str());
            remove(destination.c_str());

            if (rename(part_path.c_str(), destination.c_str()) != 0)
                MarkFailed("rename failed: " + std::string(strerror(errno)));
            else
            {
                g_state.transferred.store(total_size);
                g_state.segments_done.store(segment_count);
                SetState(STATE_SUCCESS);
                Util::RichNotify(Util::GetTick(), "Native download complete: %s", destination.c_str());
            }

            g_state.running.store(false);
            delete run;
            return nullptr;
        }

        SetState(resumed ? STATE_RESUMED : STATE_DOWNLOADING);
        g_next_segment.store(0);

        std::vector<pthread_t> threads;
        threads.reserve(kWorkerCount);

        for (int i = 0; i < kWorkerCount && i < static_cast<int>(segment_count); ++i)
        {
            WorkerArgs *args = new WorkerArgs{
                fd, host, path, total_size, kSegmentSize,
                segment_count, &done, &resume_mutex
            };

            pthread_t thread;
            if (pthread_create(&thread, nullptr, WorkerMain, args) == 0)
                threads.push_back(thread);
            else
            {
                delete args;
                MarkFailed("could not start download worker thread");
                break;
            }
        }

        for (pthread_t thread : threads)
            pthread_join(thread, nullptr);

        bool complete = !g_state.cancel.load() &&
                        g_state.segments_done.load() == segment_count;

        fsync(fd);
        close(fd);

        if (complete)
        {
            remove(resume_path.c_str());
            remove(destination.c_str());

            if (rename(part_path.c_str(), destination.c_str()) != 0)
                MarkFailed("rename failed: " + std::string(strerror(errno)));
            else
            {
                g_state.transferred.store(total_size);
                SetState(STATE_SUCCESS);
                Util::RichNotify(Util::GetTick(), "Native download complete: %s", destination.c_str());
            }
        }
        else if (!IsFailed())
        {
            MarkFailed("download interrupted; partial data was preserved for retry");
        }

        g_state.running.store(false);
        delete run;
        return nullptr;
    }

    bool Start(const std::string &url, const std::string &destination)
    {
        if (url.find("http://") != 0 && url.find("https://") != 0)
            return false;

        bool expected = false;
        if (!g_state.running.compare_exchange_strong(expected, true))
            return false;

        g_state.cancel.store(false);
        g_state.transferred.store(0);
        g_state.total.store(0);
        g_state.segments_done.store(0);
        g_state.segments_total.store(0);
        SetState(STATE_PENDING);

        RunArgs *args = new RunArgs{url, destination};
        pthread_t thread;

        if (pthread_create(&thread, nullptr, DownloadThread, args) != 0)
        {
            delete args;
            g_state.running.store(false);
            MarkFailed("could not start downloader thread");
            return false;
        }

        pthread_detach(thread);
        return true;
    }

    bool IsRunning()
    {
        return g_state.running.load();
    }

    std::string GetStateJson()
    {
        json_object *array = json_object_new_array();

        std::lock_guard<std::mutex> lock(g_state.mutex);
        if (!g_state.url.empty())
        {
            json_object *item = json_object_new_object();
            json_object_object_add(item, "path",
                                   json_object_new_string(g_state.path.c_str()));
            json_object_object_add(item, "file_size",
                                   json_object_new_uint64(g_state.total.load()));
            json_object_object_add(item, "bytes_transfered",
                                   json_object_new_uint64(g_state.transferred.load()));
            json_object_object_add(item, "state",
                                   json_object_new_string(state_strings[g_state.state]));
            json_object_object_add(item, "state_code",
                                   json_object_new_int(g_state.state));
            json_object_object_add(item, "timestamp",
                                   json_object_new_int64(g_state.timestamp));
            json_object_object_add(item, "url",
                                   json_object_new_string(g_state.url.c_str()));
            if (!g_state.error.empty())
                json_object_object_add(item, "error",
                                       json_object_new_string(g_state.error.c_str()));

            json_object_array_add(array, item);
        }

        const char *json = json_object_to_json_string(array);
        std::string out = json ? json : "[]";
        json_object_put(array);
        return out;
    }
}
