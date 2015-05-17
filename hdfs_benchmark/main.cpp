#include <iostream>
#include <getopt.h>

#include <string.h>
#include "../src/client/hdfs.h"

#include <cassert>

using namespace std;

// On Mac OS X clock_gettime is not available
#ifdef __MACH__

#include <mach/mach_time.h>

#define CLOCK_MONOTONIC 0

int clock_gettime(int clk_id, struct timespec *t) {
    mach_timebase_info_data_t timebase;
    mach_timebase_info(&timebase);
    uint64_t time;
    time = mach_absolute_time();
    double nseconds = ((double) time * (double) timebase.numer) / ((double) timebase.denom);
    double seconds = ((double) time * (double) timebase.numer) / ((double) timebase.denom * 1e9);
    t->tv_sec = seconds;
    t->tv_nsec = nseconds;
    return 0;
}

#endif

#define EXPECT_NONZERO(r, func) if(r==NULL) { \
                                    fprintf(stderr, "%s failed: %s\n", func, strerror(errno)); \
                                    exit(1); \
                                }

#define EXPECT_NONNEGATIVE(r, func) if(r < 0) { \
                                    fprintf(stderr, "%s failed: %s\n", func, strerror(errno)); \
                                    exit(1); \
                                }

timespec timespec_diff(timespec start, timespec end) {
    timespec temp;
    if ((end.tv_nsec - start.tv_nsec) < 0) {
        temp.tv_sec = end.tv_sec - start.tv_sec - 1;
        temp.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
    } else {
        temp.tv_sec = end.tv_sec - start.tv_sec;
        temp.tv_nsec = end.tv_nsec - start.tv_nsec;
    }
    return temp;
}

typedef enum {
    undefined = 0, standard, scr, zcr
} type_t;

struct {
    const char *path = NULL;
    const char *socket = "/var/lib/hadoop-hdfs/dn_socket";
    const char *namenode = "localhost";
    int namenode_port = 9000;
    size_t buffer_size = 4096;
    int verbose = false;
    int skip_checksums = false;
    int sample = false;
    type_t type = type_t::undefined;
} options;

void print_usage() {
    printf("hdfs_benchmark -f FILE [-b BUFFER_SIZE] [-t TYPE] [-n NAMENODE] [-p NAMENODE_PORT]\n"
                   "  -f, --file           File to read\n"
                   "  -b, --buffer         Buffer size, defaults to 4096\n"
                   "  -t, --type           One of standard, sc, zcr\n"
                   "  -n, --namenode       Namenode hostname, default: localhost\n"
                   "  -p, --namenode-port  Namenode port, default: 9000\n"
                   "  -v, --verbose        Verbose output, e.g. statistics, formatted speed\n"
                   "  -x, --sample         Sample the copy speed every 1s\n");
}

void parse_options(int argc, char *argv[]) {

    static struct option options_config[] = {
            {"file",           required_argument, 0,                       'f'},
            {"buffer",         optional_argument, 0,                       'b'},
            {"help",           optional_argument, 0,                       'h'},
            {"type",           optional_argument, 0,                       't'},
            {"socket",         optional_argument, 0,                       's'},
            {"namenode",       optional_argument, 0,                       'n'},
            {"namenode-port",  optional_argument, 0,                       'p'},
            {"verbose",        no_argument,       &options.verbose,        'v'},
            {"sample",         no_argument,       &options.sample,         'x'},
            {"skip-checksums", no_argument,       &options.skip_checksums, 1},

            {0, 0,                                0,                       0}
    };

    int c = 0;
    while (c >= 0) {
        int option_index;
        c = getopt_long(argc, argv, "f:b:n:p:t:s:v", options_config, &option_index);

        switch (c) {
            case 'v':
                options.verbose = true;
                break;
            case 'f':
                options.path = optarg;
                break;
            case 'b':
                options.buffer_size = atoi(optarg);
                break;
            case 'n':
                options.namenode = optarg;
                break;
            case 'p':
                options.namenode_port = atoi(optarg);
                break;
            case 'h':
                print_usage();
                exit(1);
            case 's':
                options.socket = optarg;
                break;
            case 'x':
                options.sample = true;
                break;
            case 't':
                if (strcmp(optarg, "standard") == 0) {
                    options.type = type_t::standard;
                } else if (strcmp(optarg, "scr") == 0) {
                    options.type = type_t::scr;
                } else if (strcmp(optarg, "zcr") == 0) {
                    options.type = type_t::zcr;
                } else {
                    printf("%s is not a valid type\n", optarg);
                    exit(1);
                }
                break;
            default:
                break;
        }
    }

    if (options.path == NULL) {
        print_usage();
        exit(1);
    }
}

inline void useData(void *buffer, tSize len) __attribute__((__always_inline__));

inline void useData(void *buffer, tSize len) {
    uint64_t sum = 0;
    for (size_t i = 0; i < len / sizeof(uint64_t); i++) {
        sum += *(((uint64_t *) buffer) + i);
    }
    assert(sum);
}


bool readHdfsZcr(hdfsFS fs, hdfsFile file, hdfsFileInfo *fileInfo) {
#ifdef LIBHDFS_HDFS_H
    struct hadoopRzOptions *rzOptions;
    struct hadoopRzBuffer *rzBuffer;

    // Initialize zcr
    rzOptions = hadoopRzOptionsAlloc();
    EXPECT_NONZERO(rzOptions, "hadoopRzOptionsAlloc")

    hadoopRzOptionsSetSkipChecksum(rzOptions, options.skip_checksums);
    hadoopRzOptionsSetByteBufferPool(rzOptions, ELASTIC_BYTE_BUFFER_POOL_CLASS);

    size_t total_read = 0, read = 0;
    do {
        rzBuffer = hadoopReadZero(file, rzOptions, options.buffer_size);
        if (rzBuffer != NULL) {
            const void *data = hadoopRzBufferGet(rzBuffer);
            read = hadoopRzBufferLength(rzBuffer);
            total_read += read;

            if (read > 0) {
                useData((void *) data, read);
            }

            hadoopRzBufferFree(file, rzBuffer);
        }
        else if (errno == EOPNOTSUPP) {
            printf("zcr not supported\n");
            return false;
        }
        else {
            EXPECT_NONZERO(rzBuffer, "hadoopReadZero");
        }
    } while (read > 0);

    if (total_read != fileInfo[0].mSize) {
        fprintf(stderr, "Failed to zero-copy read file to full size\n");
        exit(1);
    }

    hadoopRzOptionsFree(rzOptions);

    if(options.verbose) {
        cout << "Performed ZCR" << endl;
    }
    return true;
#else
    cout << "ZCR not supported" << endl;
    return false;
#endif
}


bool readHdfsStandard(hdfsFS fs, hdfsFile file, hdfsFileInfo *fileInfo) {
    char *buffer = (char *) malloc(sizeof(char) * options.buffer_size);
    tSize total_read = 0, read = 0;
    do {
        read = hdfsRead(fs, file, buffer, options.buffer_size);

        if (read > 0) {
            useData(buffer, read);
        }

        total_read += read;
    } while (read > 0);

    if (total_read == 0) {
        fprintf(stderr, "Failed to read any byte from the file\n");
        exit(1);
    }

    free(buffer);

    if (options.verbose) {
        cout << "Performed Standard/SCR" << endl;
    }
    return true;
}

int main(int argc, char *argv[]) {
    parse_options(argc, argv);

    if (options.verbose) {
        cout << "Namenode:  " << options.namenode << ":" << options.namenode_port << endl;
        cout << "Socket:    " << options.socket << endl;
        cout << "File:      " << options.path << endl;
        cout << "Buffer:    " << options.buffer_size << endl;
        cout << "Checksums: " << (options.skip_checksums ? "false" : "true") << endl;
        cout << "Type:      " << options.type << endl;
    }

    struct hdfsBuilder *hdfsBuilder = hdfsNewBuilder();
    hdfsBuilderSetNameNode(hdfsBuilder, options.namenode);
    hdfsBuilderSetNameNodePort(hdfsBuilder, options.namenode_port);
    if (options.type == type_t::undefined || options.type == type_t::scr || options.type == type_t::zcr) {
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit", "true");
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.domain.socket.path", options.socket);
        // TODO Test
        //hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.domain.socket.data.traffic", "true");
        //hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit.streams.cache.size", "4000");
        hdfsBuilderConfSetStr(hdfsBuilder, "dfs.client.read.shortcircuit.skip.checksum",
                              options.skip_checksums > 0 ? "true" : "false");
    }

    // Connect
    hdfsFS fs = hdfsBuilderConnect(hdfsBuilder);
    EXPECT_NONZERO(fs, "hdfsBuilderConnect")

    // Check if the file exists
    if (hdfsExists(fs, options.path) != 0) {
        printf("File %s does not exist\n", options.path);
    } else {
        hdfsFileInfo *fileInfo = hdfsGetPathInfo(fs, options.path);

        char ***hosts = hdfsGetHosts(fs, options.path, 0, fileInfo->mSize);
        EXPECT_NONZERO(hosts, "hdfsGetHosts")

        uint i = 0;
        for (i = 0; hosts[i]; i++) {
            cout << "Block[" << i << "]" << endl;
            for (uint j = 0; hosts[i][j]; j++)
                cout << "\t[" << j << "]: " << hosts[i][j] << endl;
        }
        cout << "Reading " << i << " blocks" << endl;


        // Open and read the file
        //hdfsFile file = hdfsOpenFile2(fs, "ubuntu", options.path, O_RDONLY, options.buffer_size, 0, 0);
        //EXPECT_NONZERO(file, "hdfsOpenFile")



        hdfsFreeFileInfo(fileInfo, 1);
        //hdfsCloseFile(fs, file);
    }

    hdfsDisconnect(fs);
    hdfsFreeBuilder(hdfsBuilder);

    return 0;
}
