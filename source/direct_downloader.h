#ifndef EZ_DIRECT_DOWNLOADER_H
#define EZ_DIRECT_DOWNLOADER_H

#include <string>

namespace DirectDownloader
{
    // Queue a native PS5-side download. Only one native direct download is
    // allowed at a time.
    bool Start(const std::string &url, const std::string &destination);

    bool IsRunning();

    // JSON array compatible with the existing download-status UI shape.
    std::string GetStateJson();
}

#endif
