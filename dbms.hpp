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

//Type of Column
enum class col_type {
    boolean,    //1 Byte
    integer,    //4 Bytes
    number,     //8 Bytes
    varchar,    //2 Bytes (pointer)
    // time,       //8 bytes
    // date,       //4 bytes
    null        //0 Bytes
};

using base = std::variant<bool, int, double, std::string>;
using table_row = std::vector<base>;

//Basic Data Structure. (Supports all the functions like join and filter)
struct table_data{
    std::vector<table_row> data;
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

//Request Structure
struct request {
    request_type main = request_type::null;
    std::string group;
    std::string entity;
    std::vector<std::string> args;
    std::vector<table_row> data;
    std::chrono::duration<double> start_time, end_time;
    std::promise<table_data> result;
};

//Main Database Object
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
        std::future<table_data> task(request req);
};