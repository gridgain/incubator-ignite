/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <fstream>
#include <climits>

#include <boost/thread/thread.hpp>
#include <boost/interprocess/sync/interprocess_semaphore.hpp>
#include <boost/random/random_device.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <boost/limits.hpp>
#include <boost/program_options.hpp>

#include <ignite/binary/binary.h>

#include <ignite/thin/ignite_client_configuration.h>
#include <ignite/thin/ignite_client.h>

using namespace ignite::thin;

class SampleValue
{
    friend struct ignite::binary::BinaryType<SampleValue>;
public:
    SampleValue() :
        id(0)
    {
        // No-op.
    }

    SampleValue(int32_t id) :
        id(id)
    {
        // No-op.
    }

private:
    int32_t id;
};

namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<SampleValue>
        {
            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(SampleValue)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(SampleValue)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_IS_NULL_FALSE(SampleValue)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(SampleValue)

            static void Write(BinaryWriter& writer, const SampleValue& obj)
            {
                BinaryRawWriter raw = writer.RawWriter();

                raw.WriteInt32(obj.id);
            }

            static void Read(BinaryReader& reader, SampleValue& dst)
            {
                BinaryRawReader raw = reader.RawReader();

                dst.id = raw.ReadInt32();
            }
        };
    }
}

struct BenchmarkConfiguration
{
    BenchmarkConfiguration() :
        threadNum(boost::thread::hardware_concurrency()),
        iterationsNum(100000),
        warmupIterationsNum(100000),
        log(0)
    {
        // No-op.
    }

    int32_t threadNum;
    int32_t iterationsNum;
    int32_t warmupIterationsNum;
    std::ostream* log;
};

class BenchmarkBase
{
public:
    BenchmarkBase(const BenchmarkConfiguration& cfg):
        cfg(cfg)
    {
        // No-op.
    }

    virtual ~BenchmarkBase()
    {
        // No-op.
    }

    virtual void SetUp() = 0;

    virtual bool Test() = 0;

    virtual void TearDown() = 0;

    virtual std::string GetName() = 0;

    const BenchmarkConfiguration& GetConfig() const
    {
        return cfg;
    }

    void GenerateRandomSequence(std::vector<int32_t>& res, int32_t num, int32_t min = 0, int32_t max = INT32_MAX)
    {
        res.clear();
        res.reserve(num);
        
        boost::random_device seed_gen; 
        boost::random::mt19937 gen(seed_gen());
        boost::random::uniform_int_distribution<int32_t> dist(min, max);

        for (int32_t i = 0; i < num; ++i)
            res.push_back(dist(gen));
    }

    void FillCache(cache::CacheClient<int32_t, SampleValue>& cache, int32_t min, int32_t max)
    {
        boost::random_device seed_gen; 
        boost::random::mt19937 gen(seed_gen());
        boost::random::uniform_int_distribution<int32_t> dist;

        for (int32_t i = min; i < max; ++i)
            cache.Put(i, SampleValue(dist(gen)));
    }

protected:
    const BenchmarkConfiguration cfg;
};

class ClientCacheBenchmarkAdapter : public BenchmarkBase
{
public:
    ClientCacheBenchmarkAdapter(
        const BenchmarkConfiguration& cfg,
        IgniteClient& client,
        const std::string& cacheName) :
        BenchmarkBase(cfg),
        client(client),
        cache(client.GetOrCreateCache<int32_t, SampleValue>(cacheName.c_str()))
    {
        // No-op.
    }

    virtual ~ClientCacheBenchmarkAdapter()
    {
        // No-op.
    }

    virtual void SetUp()
    {
        cache.RemoveAll();
        cache.Clear();
    }

    virtual void TearDown()
    {
        cache.RemoveAll();
        cache.Clear();
    }

protected:
    IgniteClient& client;

    cache::CacheClient<int32_t, SampleValue> cache;
};

class ClientCachePutBenchmark : public ClientCacheBenchmarkAdapter
{
public:
    ClientCachePutBenchmark(const BenchmarkConfiguration& cfg, IgniteClient& client) :
        ClientCacheBenchmarkAdapter(cfg, client, "PutBenchTestCache"),
        iteration(0)
    {
        // No-op.
    }

    virtual ~ClientCachePutBenchmark()
    {
        // No-op.
    }

    virtual void SetUp()
    {
        ClientCacheBenchmarkAdapter::SetUp();

        GenerateRandomSequence(keys, GetConfig().iterationsNum);
        GenerateRandomSequence(values, GetConfig().iterationsNum);
    }

    virtual void TearDown()
    {
        ClientCacheBenchmarkAdapter::TearDown();
    }

    virtual bool Test()
    {
        cache.Put(keys[iteration], SampleValue(values[iteration]));

        ++iteration;

        return iteration < GetConfig().iterationsNum;
    }

    virtual std::string GetName()
    {
        return "Thin client Put";
    }

private:
    std::vector<int32_t> keys;
    std::vector<int32_t> values;
    int32_t iteration;
};

class ClientCacheGetBenchmark : public ClientCacheBenchmarkAdapter
{
public:
    ClientCacheGetBenchmark(const BenchmarkConfiguration& cfg, IgniteClient& client) :
        ClientCacheBenchmarkAdapter(cfg, client, "GutBenchTestCache"),
        iteration(0)
    {
        // No-op.
    }

    virtual ~ClientCacheGetBenchmark()
    {
        // No-op.
    }

    virtual void SetUp()
    {
        ClientCacheBenchmarkAdapter::SetUp();
        
        GenerateRandomSequence(keys, GetConfig().iterationsNum, 0, 10000);

        FillCache(cache, 0, 10000);
    }

    virtual void TearDown()
    {
        ClientCacheBenchmarkAdapter::TearDown();
    }

    virtual bool Test()
    {
        SampleValue val;
        cache.Get(keys[iteration], val);

        ++iteration;

        return iteration < GetConfig().iterationsNum;
    }

    virtual std::string GetName()
    {
        return "Thin client Get";
    }

private:
    std::vector<int32_t> keys;
    int32_t iteration;
};


void PrintBackets(const std::string& annotation, std::vector<int64_t>& res, std::ostream& log)
{
    std::sort(res.begin(), res.end());

    log << annotation << ": "
        << "min: " << res.front() << "us, "
        << "10%: " << res.at(static_cast<size_t>(res.size() * 0.1)) << "us, "
        << "20%: " << res.at(static_cast<size_t>(res.size() * 0.2)) << "us, "
        << "50%: " << res.at(static_cast<size_t>(res.size() * 0.5)) << "us, "
        << "90%: " << res.at(static_cast<size_t>(res.size() * 0.9)) << "us, "
        << "95%: " << res.at(static_cast<size_t>(res.size() * 0.95)) << "us, "
        << "99%: " << res.at(static_cast<size_t>(res.size() * 0.99)) << "us, "
        << "max: " << res.back() << "us"
        << std::endl;
}

void MeasureThread(
    boost::interprocess::interprocess_semaphore& sem,
    std::vector<int64_t>& latency,
    BenchmarkBase& bench)
{
    using namespace boost::chrono;

    sem.wait();

    latency.clear();

    latency.reserve(bench.GetConfig().iterationsNum);

    auto begin = steady_clock::now();

    bool run = true;

    while (run)
    {
        auto putBegin = steady_clock::now();

        run = bench.Test();

        latency.push_back(duration_cast<microseconds>(steady_clock::now() - putBegin).count());
    }
}

template<typename T>
int64_t MeasureInThreads(
    const BenchmarkConfiguration& cfg,
    IgniteClient& client,
    std::vector<int64_t>& latency)
{
    using namespace boost::chrono;

    std::vector<T> contexts;
    std::vector<boost::thread> threads;
    std::vector< std::vector<int64_t> > latencies(cfg.threadNum);

    contexts.reserve(cfg.threadNum);
    threads.reserve(cfg.threadNum);

    boost::interprocess::interprocess_semaphore sem(0);

    for (int32_t i = 0; i < cfg.threadNum; ++i)
    {
        contexts.push_back(T(cfg, client));

        contexts[i].SetUp();

        threads.push_back(boost::thread(MeasureThread, boost::ref(sem), boost::ref(latencies[i]), boost::ref(contexts[i])));
    }

    auto begin = steady_clock::now();

    for (int32_t i = 0; i < cfg.threadNum; ++i)
        sem.post();

    for (int32_t i = 0; i < cfg.threadNum; ++i)
        threads[i].join();
    
    for (int32_t i = 0; i < cfg.threadNum; ++i)
        contexts[i].TearDown();

    using namespace boost::chrono;

    auto duration = steady_clock::now() - begin;

    latency.clear();
    latency.reserve(cfg.iterationsNum * cfg.threadNum);

    for (int32_t i = 0; i < cfg.threadNum; ++i)
        latency.insert(latency.end(), latencies[i].begin(), latencies[i].end());   

    return duration_cast<milliseconds>(duration).count();
}

template<typename T>
void Run(const std::string& annotation, const BenchmarkConfiguration& cfg, const IgniteClientConfiguration& clientCfg)
{
    std::ostream* log = cfg.log;

    if (log)
        *log << "Warming up. Operations number: " << cfg.warmupIterationsNum << std::endl;

    IgniteClient client = IgniteClient::Start(clientCfg);

    std::vector<int64_t> latency;
    int64_t duration = MeasureInThreads<T>(cfg, client, latency);
    
    if (log)
        *log << std::endl << "Starting benchmark. Operations number: " << cfg.iterationsNum << std::endl;

    duration = MeasureInThreads<T>(cfg, client, latency);

    if (log)
    {
        PrintBackets(annotation, latency, *log);

        *log << std::endl << "Duration: " << duration << "ms" << std::endl;

        *log << "Throughput: " << static_cast<int32_t>((cfg.iterationsNum * 1000.0) / duration) << "op/sec" << std::endl;
    }
}

int main(int argc, const char* argv[])
{
    using namespace boost::program_options;

    try
    {
        options_description desc("Options");

        desc.add_options()
            ("help,h", "Help screen")
            ("get,g", "Run Get() benchmark")
            ("put,p", "Run Put() benchmark")
            ("log_dir,l", value<std::string>()->default_value(""), "Logs output directory")
            ("address,a", value<std::string>()->required(), "Address. Format: \"address.com[port[..range]][,...]\"")
            ("warmup_runs,w", value<int32_t>()->default_value(10000), "Warmup runs number")
            ("runs,r", value<int32_t>()->default_value(10000), "Measure runs number")
            ("threads,t", value<int32_t>()->default_value(boost::thread::hardware_concurrency()), "Threads number");

        variables_map vm;
        store(parse_command_line(argc, argv, desc), vm);
        vm.notify();

        if (vm.count("help"))
        {
            std::cout << desc << std::endl;

            return 0;
        }

        if ((!vm.count("get") && !vm.count("put")) || (vm.count("get") && vm.count("put")))
        {
            std::cout << "Please, specify --get or --put." << std::endl;
            std::cout << desc << std::endl;

            return -1;
        }

        std::string logDir = vm["log_dir"].as<std::string>();

        std::ofstream log(logDir + "thin_bench.log");

        BenchmarkConfiguration cfg;
        cfg.iterationsNum = vm["runs"].as<int32_t>();
        cfg.warmupIterationsNum = vm["warmup_runs"].as<int32_t>();
        cfg.threadNum = vm["threads"].as<int32_t>();
        cfg.log = &log;

        IgniteClientConfiguration clientCfg;
        clientCfg.SetEndPoints(vm["address"].as<std::string>());

        log << "log_dir: " << logDir << std::endl;
        log << "address: " << clientCfg.GetEndPoints() << std::endl;
        log << "runs: " << cfg.iterationsNum << std::endl;
        log << "warmup_runs: " << cfg.warmupIterationsNum << std::endl;
        log << "threads: " << cfg.threadNum << std::endl;

        if (vm.count("put"))
            Run<ClientCachePutBenchmark>("Put", cfg, clientCfg);

        if (vm.count("get"))
            Run<ClientCacheGetBenchmark>("Get", cfg, clientCfg);
    }
    catch (const error &ex)
    {
        std::cerr << ex.what() << std::endl;

        return -1;
    }

    return 0;
}