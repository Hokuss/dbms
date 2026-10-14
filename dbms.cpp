#include "dbms.hpp"
#include <filesystem>
#include <future>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
namespace fs = std::filesystem;
using namespace std::chrono;

namespace utils {
    
}

namespace{
    std::vector<std::string> tables;
    fs::path dbms_path;

    struct col_meta {
        std::string name;
        int type;
        int size;
        int offset; // How many bytes from the start of the row this column begins
    };

    struct table_schema{
        int total_row_size = 0;
        int create_time = -1;
        int update_time = -1;
        std::string unique_key;
        std::vector<col_meta> columns;
    };

    std::unordered_map<std::string, table_schema> schema_cache;

    struct group_schema {
        std::unordered_map<std::string, std::string> args;
    };
    
}

duration<double> current_time() {
    auto now = high_resolution_clock::now();
    duration<double> epoch_duration = now.time_since_epoch();
    return epoch_duration;
}

dbms::dbms(unsigned int id) {
    this->database_name = std::to_string(id);
    orch_thread = std::thread(&dbms::loop, this);
}

dbms::~dbms(){
    active = false;
    queue_cv.notify_all();
    if (orch_thread.joinable()) {
        orch_thread.join();
    }
}

//Task Submitter
std::future<std::vector<std::vector<db_value>>> dbms::task(request req){
    auto fut = req.result.get_future();
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        task_queue.push(std::move(req));
    }
    queue_cv.notify_one();
    return fut;
}

//Task Reader to offload it to the DBME
void dbms::loop() {
    dbms_path = fs::current_path() / ("." + this->database_name);
    if(!fs::exists(dbms_path)){
        fs::create_directory(dbms_path);
    }
    while (active) {
        request task;
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            queue_cv.wait(lock, [this] {return !task_queue.empty() || !active;});

            task = std::move(task_queue.front());
            task_queue.pop();
        }
        task.start_time = current_time();
        std::thread(&dbms::task_fn, this, std::move(task)).detach();
    }
}

void task_fn(request req){
    //Test the table
    fs::path group_path = dbms_path/req.group;
    fs::path table_path = group_path/req.table;
    if(!fs::exists(group_path) & (req.main!=request_type::create_group)) {
        req.success = false;
        req.result.set_value({{"Error:Group Not found"}});
        return;
    } else if(!fs::exists(group_path)) {
        if(!fs::create_directory(group_path)) req.result.set_value({{"Group Creation failed!"}});
        else req.result.set_value({{"Group Created"}});

        settings(group_path);
        return;
    }

    if (!fs::exists()) {
    
    }
}
