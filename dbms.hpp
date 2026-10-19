#pragma once

#include <future>
#include <vector>
#include <thread>
#include <atomic>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <variant>
#include <chrono>


using db_value = std::variant<int, float, double, bool, std::string>;

enum class columntype{
    number,
    date,
    timestamp,
    text,
    boolean
};

struct column{
    std::string name = "";
    columntype type = columntype::text;
    unsigned int size = 4;
};

enum class request_type {
    null,
    create_group,
    set_group_rule,
    create_table,
    set_table_rule,
    get_rows,
    insert_rows,
    delete_rows
};

struct request {
    request_type main = request_type::null;
    std::string table;
    std::string group;
    std::vector<std::string> args;
    std::chrono::duration<double> start_time, end_time;
    bool success = true;
    std::promise<std::vector<std::vector<db_value>>> result;
};


class dbms{
    private:
        // To make things run on seperate thread
        void loop();
        std::atomic<bool> active{true};

        std::thread orch_thread;
        std::queue<request> task_queue;
        std::mutex queue_mutex;

        std::condition_variable queue_cv;

        
        void task_fn(request req);
        std::string database_name; 

    public:
        dbms(unsigned int id);
        ~dbms();

        //Wrapers
        std::future<std::vector<std::vector<db_value>>> task(request req);
};