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
#include <random>
#include <thread>
#include <condition_variable>

#include <ignite/binary/binary.h>
#include <ignite/thin/ignite_client_configuration.h>
#include <ignite/thin/ignite_client.h>

using namespace ignite::thin;

class Semaphore
{
public:
    Semaphore(uint64_t init = 0) :
        count(init)
    {
        // No-op.
    }

    void Post()
    {
        std::lock_guard<std::mutex> lock(mutex);
        ++count;
        condition.notify_one();
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lock(mutex);

        while(!count)
            condition.wait(lock);

        --count;
    }

private:
    std::mutex mutex;
    std::condition_variable condition;
    uint64_t count = 0;
};

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

const int32_t WARMUP_ITERATIONS_NUM = 100000;
const int32_t ITERATIONS_NUM = 1000000;
const int32_t THREAD_NUM = 1;
const std::string ADDRESS = "127.0.0.1";

struct BenchmarkConfiguration
{
    BenchmarkConfiguration() :
        threadNum(THREAD_NUM),
        iterationsNum(ITERATIONS_NUM),
        warmupIterationsNum(WARMUP_ITERATIONS_NUM),
        log(&std::cout),
        endPoints(ADDRESS)
    {
        // No-op.
    }

    uint64_t threadNum;
    uint64_t iterationsNum;
    uint64_t warmupIterationsNum;
    std::ostream* log;
    std::string endPoints;
};

class LatencyBenchmark
{
public:
    LatencyBenchmark(uint64_t iterations) :
        iteration(0),
        iterations(iterations),
        latency()
    {
        latency.reserve(iterations);
    }

    virtual ~LatencyBenchmark()
    {
        // No-op.
    }

    virtual void SetUp() = 0;

    virtual void TearDown() = 0;

    virtual std::string GetName() = 0;

    static void GenerateRandomSequence(std::vector<int32_t>& res, uint64_t num, int32_t min = 0, int32_t max = INT32_MAX)
    {
        res.clear();
        res.reserve(num);

        std::random_device seed_gen;
        std::mt19937 gen(seed_gen());
        std::uniform_int_distribution<int32_t> dist(min, max);

        for (int32_t i = 0; i < num; ++i)
            res.push_back(dist(gen));
    }

    static void FillCache(cache::CacheClient<int32_t, SampleValue>& cache, int32_t min, int32_t max)
    {
        std::random_device seed_gen;
        std::mt19937 gen(seed_gen());
        std::uniform_int_distribution<int32_t> dist;

        for (int32_t i = min; i < max; ++i)
            cache.Put(i, SampleValue(dist(gen)));
    }

    uint64_t GetIterations() const
    {
        return iterations;
    }

    uint64_t GetIteration() const
    {
        return iteration;
    }

    const std::vector<int64_t>& GetLatency() const
    {
        return latency;
    }

    bool Test()
    {
        if (iteration >= iterations)
            return false;

        using namespace std::chrono;

        auto begin = steady_clock::now();

        DoTest();

        latency.push_back(duration_cast<microseconds>(steady_clock::now() - begin).count());

        ++iteration;

        return true;
    }

protected:
    virtual void DoTest() = 0;

private:
    uint64_t iteration;
    uint64_t iterations;
    std::vector<int64_t> latency;
};

class ClientCacheBenchmarkAdapter : public LatencyBenchmark
{
public:
    ClientCacheBenchmarkAdapter(const IgniteClientConfiguration& clientCfg, const std::string& cacheName, uint64_t iterations) :
        LatencyBenchmark(iterations),
        client(IgniteClient::Start(clientCfg)),
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
    IgniteClient client;

    cache::CacheClient<int32_t, SampleValue> cache;
};

class ClientCachePutBenchmark : public ClientCacheBenchmarkAdapter
{
public:
    ClientCachePutBenchmark(const IgniteClientConfiguration& clientCfg, uint64_t iterations) :
        ClientCacheBenchmarkAdapter(clientCfg, "PutBenchTestCache", iterations)
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

        GenerateRandomSequence(keys, GetIterations());
        GenerateRandomSequence(values, GetIterations());
    }

    virtual void TearDown()
    {
        ClientCacheBenchmarkAdapter::TearDown();
    }

    virtual void DoTest()
    {
        cache.Put(keys[GetIteration()], SampleValue(values[GetIteration()]));
    }

    virtual std::string GetName()
    {
        return "Thin client Put";
    }

private:
    std::vector<int32_t> keys;
    std::vector<int32_t> values;
};

class ClientCacheGetBenchmark : public ClientCacheBenchmarkAdapter
{
public:
    ClientCacheGetBenchmark(const IgniteClientConfiguration& clientCfg, uint64_t iterations) :
        ClientCacheBenchmarkAdapter(clientCfg, "GutBenchTestCache", iterations)
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

        GenerateRandomSequence(keys, GetIterations(), 0, 10000);

        FillCache(cache, 0, 10000);
    }

    virtual void TearDown()
    {
        ClientCacheBenchmarkAdapter::TearDown();
    }

    virtual void DoTest()
    {
        SampleValue val;
        cache.Get(keys[GetIteration()], val);
    }

    virtual std::string GetName()
    {
        return "Thin client Get";
    }

private:
    std::vector<int32_t> keys;
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

void MeasureThread(Semaphore& sem, LatencyBenchmark& bench)
{
    sem.Wait();

    bool run = true;

    while (run)
    {
        run = bench.Test();
    }
}

template<typename T>
void MeasureInThreads(
    const IgniteClientConfiguration& clientCfg,
    std::vector<int64_t>& latency,
    uint64_t threadNum,
    uint64_t iterations)
{
    std::vector<T> contexts;
    std::vector<std::thread> threads;

    contexts.reserve(threadNum);
    threads.reserve(threadNum);

    Semaphore sem(0);

    for (uint64_t i = 0; i < threadNum; ++i)
    {
        contexts.push_back(T(clientCfg, iterations));

        contexts[i].SetUp();

        threads.push_back(std::thread(MeasureThread, std::ref(sem), std::ref(contexts[i])));
    }

    for (uint64_t i = 0; i < threadNum; ++i)
        sem.Post();

    for (uint64_t i = 0; i < threadNum; ++i)
        threads[i].join();

    for (uint64_t i = 0; i < threadNum; ++i)
        contexts[i].TearDown();

    latency.clear();
    latency.reserve(iterations * threadNum);

    for (uint64_t i = 0; i < threadNum; ++i)
        latency.insert(latency.end(), contexts[i].GetLatency().begin(), contexts[i].GetLatency().end());
}

template<typename T>
void Run(const std::string& annotation, const BenchmarkConfiguration& cfg)
{
    std::ostream* log = cfg.log;

    if (log)
        *log << "Server endpoints: " << cfg.endPoints << std::endl;

    IgniteClientConfiguration clientCfg;
    clientCfg.SetEndPoints(cfg.endPoints);

    std::vector<int64_t> latency;

    if (log)
        *log << "Warming up. Operations number: " << cfg.warmupIterationsNum << std::endl;

    MeasureInThreads<T>(clientCfg, latency, cfg.threadNum, cfg.warmupIterationsNum);

    if (log)
        *log << std::endl << "Starting benchmark. Operations number: " << cfg.iterationsNum << std::endl;

    MeasureInThreads<T>(clientCfg, latency, cfg.threadNum, cfg.warmupIterationsNum);

    if (log)
        PrintBackets(annotation, latency, *log);
}

void PrintHelp(const std::string& bin)
{
    std::cout << "Usage: " << bin << " <command> [<option>[...]]" << std::endl;
    std::cout << "Possible commands:" << std::endl;
    std::cout << " help      : Show this message" << std::endl;
    std::cout << " get       : Run 'get' benchmark" << std::endl;
    std::cout << " put       : Run 'put' benchmark" << std::endl;
    std::cout << "Possible options:" << std::endl;
    std::cout << " -t <N>    : Number of threads. Default is " << THREAD_NUM << std::endl;
    std::cout << " -w <N>    : Number of operations on warm up stage. Default is " << WARMUP_ITERATIONS_NUM << std::endl;
    std::cout << " -i <N>    : Number of operations on benchmark stage. Default is " << ITERATIONS_NUM << std::endl;
    std::cout << " -a <ips>  : Endpoints list. Default is " << ADDRESS << std::endl;
    std::cout << std::endl;
}

int ParseConfig(BenchmarkConfiguration& cfg, int argc, char* argv[])
{
    // Parsing arguments from 2 to N in pairs
    for (int i = 2; i < argc; i+=2)
    {
        std::string opt(argv[i]);
        std::string val(argv[i+1]);

        if (opt == "-a")
        {
            cfg.endPoints = val;

            continue;
        }

        uint64_t nval = ignite::common::LexicalCast<uint64_t>(val);

        if (opt == "-t")
            cfg.threadNum = nval;
        else
        if (opt == "-w")
            cfg.warmupIterationsNum = nval;
        else
        if (opt == "-i")
            cfg.iterationsNum = nval;
        else
            return -5;
    }

    return 0;
}

int main(int argc, char* argv[])
{
    std::string binName = argv[0];

    size_t pos = binName.find_last_of("\\/");

    binName = binName.substr(pos == std::string::npos ? 0 : pos + 1);

    if (argc < 2 || (argc % 2) != 0)
    {
        PrintHelp(binName);

        return -1;
    }

    BenchmarkConfiguration cfg;

    int res = ParseConfig(cfg, argc, argv);

    if (res)
    {
        PrintHelp(binName);

        return res;
    }

    std::string cmd(argv[1]);

    try
    {
        if (cmd == "get")
            Run<ClientCacheGetBenchmark>("Get", cfg);
        else
        if (cmd == "put")
            Run<ClientCachePutBenchmark>("Put", cfg);
        else
        {
            PrintHelp(binName);

            return cmd == "help" ? 0 : -1;
        }
    }
    catch (std::exception& err)
    {
        std::cout << "Error: " << err.what() << std::endl;

        return -2;
    }
    catch (...)
    {
        std::cout << "Unknown error" << std::endl;

        return -3;
    }

    return 0;
}