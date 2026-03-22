#pragma once

#include <vector>
#include <thread>
#include <atomic>
#include <queue>
#include <mutex>
#include <future>
#include <memory>
#include <condition_variable>
#include <variant>


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


class dbms{
    private:
        // To make things run on seperate thread
        void loop();
        std::atomic<bool> active{true};

        std::thread dbms_thread;
        std::queue<std::function<void()>> task_queue;
        std::mutex queue_mutex;

        std::condition_variable queue_cv;

        std::string database_name; 

        //Task offloader
        template<typename F>
        auto submit_task(F&& f) -> std::future<typename std::invoke_result<F>::type>;

        //Actual Functions that run on the seperate thread
        bool creation_table(std::string table_name, std::vector<column> define);
        bool irow(std::string table_name, std::vector<db_value> row_data);

        bool load_schema(const std::string& table_name);

        std::vector<db_value> grow(std::string table_name, std::pair<std::string,db_value> request);
        bool drow(std::string table_name, std::pair<std::string, db_value> request);

    public:
        dbms(unsigned int id);
        ~dbms();

        //Wrapers
        std::future<bool> create_table(std::string table_name, std::vector<column> define);
        std::future<bool> insert_row(std::string table_name, std::vector<db_value> row_data);
        std::future<bool> delete_row(std::string table_name, std::pair<std::string, db_value> request);
        std::future<std::vector<db_value>> get_row(std::string table_name, std::pair<std::string,db_value> request);

};

template<typename F>
auto dbms::submit_task(F&& f) -> std::future<typename std::invoke_result<F>::type> {
    
    using return_type = typename std::invoke_result<F>::type;
    auto task = std::make_shared<std::packaged_task<return_type()>>(std::forward<F>(f));
    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        task_queue.emplace([task]() { 
            (*task)();
        });
    }
    queue_cv.notify_one();
    return res;
}