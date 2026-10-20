#include "dbms.hpp"
#include <charconv>
#include <exception>
#include <filesystem>
#include <fstream>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>
namespace fs = std::filesystem;
using namespace std::chrono;

using base = std::variant<bool, int, double, std::string>;
using table_row = std::vector<base>;

namespace{
    std::vector<std::string> tables;
    fs::path dbms_path;


    enum class schema_type {
        table,
        unstruct,
        file,
        group,
        database,
        null
    };

    class schema {
        public:
            std::map<std::string, base> args;
            schema_type type;
            fs::path schema_path;
            schema(fs::path pth, schema_type b, std::map<std::string,base> a = {}){
                check(a);
                args = std::move(a);
                type = b;
                apply_defaults();
                schema_path = pth;
            }
            schema(fs::path pth){
                std::ifstream cnfg(pth, std::ios::binary);
                if(!cnfg.is_open()){
                    throw std::exception();
                }
                schema_path = pth;
                std::string buffer;
                //Add the line to get the schema type from the file as well

                while(std::getline(cnfg,buffer)){
                    int colon_pos = buffer.find(':');
                    int first_pos = buffer.find('"');
                    int second_pos = buffer.find('"', first_pos+1);
                    int third_pos = buffer.find('"', colon_pos);
                    int fourth_pos = buffer.find('"', third_pos+1);
                    if(second_pos>=colon_pos){
                        throw std::runtime_error("Incorrect Cnfiguration File");
                    }
                    std::string key = buffer.substr(first_pos+1,second_pos-first_pos-1);
                    base value = parseValue(buffer.substr(third_pos, fourth_pos-third_pos-1));
                    args[key] = value;
                }
                cnfg.close();
            }
            ~schema(){
                std::ofstream cnfg(schema_path, std::ios::binary|std::ios::trunc);
                for (auto& itr: args) {
                    std::string buffer = '"'+itr.first+'"';
                    buffer += ':';
                    buffer += '"' + parsebase(itr.second) + '"';
                    buffer += '\n';
                    cnfg.write(buffer.c_str(),buffer.size());
                }
                cnfg.close();
            }
        private:
            bool check(std::map<std::string,base> a){
                return true;
            }
            void apply_defaults(){
                return;
            }
            base parseValue(const std::string& s) {
                int i{};
                auto [p1, ec1] = std::from_chars(s.data(), s.data() + s.size(), i);
                if (ec1 == std::errc{} && p1 == s.data() + s.size()) return i;

                double d{};
                auto [p2, ec2] = std::from_chars(s.data(), s.data() + s.size(), d);
                if (ec2 == std::errc{} && p2 == s.data() + s.size()) return d;

                if (s == "true")  return true;
                if (s == "false") return false;
                return s;
            }
            std::string parsebase(const base& b) {
                if (auto* p = std::get_if<bool>(&b))        return *p ? "true" : "false";
                if (auto* p = std::get_if<int>(&b))         return std::to_string(*p);
                if (auto* p = std::get_if<double>(&b))      return std::to_string(*p);
                if (auto* p = std::get_if<std::string>(&b)) return *p;
                return "";
            }
    };

    class schema_cache{
        public:
            std::shared_ptr<schema> get_schema(fs::path pth){
                if(holder.find(pth)!=holder.end()) return holder[pth];
                std::shared_ptr<schema> new_sch = std::make_shared<schema>(schema(pth));
                holder[pth] = new_sch;
                return new_sch;
            }

            void set_schema(std::shared_ptr<schema> sch){
                holder[sch->schema_path] = sch;
            }

            void remove(fs::path pth){
                auto it = holder.find(pth);
                if(it!= holder.end()) holder.erase(it);
            }
        private:
            std::unordered_map<fs::path, std::shared_ptr<schema>> holder;
    };

    schema_cache cacheschema;
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

        //Write the meta data file for Database as well
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

void rule_set(fs::path rule_pth, std::vector<std::string> rules, schema_type type){
    //Implement the rule procedure. 
    
    std::shared_ptr<schema> sch;
    try {
        sch = cacheschema.get_schema(rule_pth);
    } catch (const std::exception &E) {
        sch = std::make_shared<schema>(schema(rule_pth,type));
        cacheschema.set_schema(sch);
    }
}

void dbms::task_fn(request req){
    //Test the table
    fs::path group_path = dbms_path/req.group;
    fs::path table_path = group_path/req.table;

    switch (req.main) {
        case request_type::create_group:
            if(!fs::create_directory(group_path)) {
                req.result.set_value({{"Failed to create Group"}});
                return;
            }
        case request_type::set_group_rule:
            rule_set(group_path/"cnfg.bin", req.args, schema_type::group);
            break;
        case request_type::create_table:
            if(!fs::exists(group_path)){
                req.result.set_value({{"Specified Group Doesn't Exist"}});
                return;
            }
            if(!fs::create_directory(table_path)){
                req.result.set_value({{"Failed to create Table"}});
                return;
            }
        case request_type::set_table_rule:
            rule_set(table_path, req.args, schema_type::table);
            break;

        
        default:
            break;
    }

}
