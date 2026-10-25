#include "dbms.hpp"
#include <charconv>
#include <cstddef>
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

using fut_proc = std::variant<std::vector<std::string>, std::vector<table_row>>;

namespace{
    std::vector<std::string> tables;
    fs::path dbms_path;

    enum class schema_type {
        meta,
        config,
        columns,
        access,
        error
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
                if(pth.filename()=="column.bin") type = schema_type::columns;
                else if (pth.filename()=="cnfg.bin") type = schema_type::config;
                else if (pth.filename()=="meta.bin") type = schema_type::meta;
                else type = schema_type::error;

                std::string buffer;
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

            std::vector<std::pair<std::string,col_type>> get_columns(){
                if (type!=schema_type::columns) {
                    throw std::logic_error("get_columns() called on a schema type that is not a column definition.");
                }
                std::vector<std::pair<std::string, col_type>> result;
                for (auto it : args) {
                    result.push_back(std::make_pair(it.first, parse_cols(it.second)));
                }
                return result;
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
            col_type parse_cols(base a){
                if(a.index()==2) {
                    return col_type(std::get<int>(a));
                }
                else if (a.index()==4) {
                    std::string temp = std::get<std::string>(a);
                    if(temp=="boolean") return col_type::boolean;
                    else if (temp=="integer") return col_type::integer;
                    else if (temp=="number") return col_type::number;
                    else if (temp=="varchar") return col_type::varchar;
                    // else if (temp=="date") return col_type::date;
                    // else if (temp=="time") return col_type::time;
                    else return col_type::null; 
                } else return col_type::null;
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

    enum class entity_type{
        table,
        stages,
        graphs,
        vecotrs,
        vgraphs
    };

    enum class task_type{
        insertr,
        delr,
        getr
    };

    struct bkg_request{
        std::string group;
        entity_type ent_type;
        std::string entity;
        task_type t;
        fut_proc a;
    };

    std::queue<bkg_request> bkg_queue;
    std::mutex bkg_mutex;
}

namespace utils{
    void trim(std::string_view &a){
        if (a.size()<=1) return;
        int i = 0, j = a.size()-1;
        for (i = 0; i<a.size(); i++) {
            if(a[i]==' ' | a[i]=='-' | a[i]=='"') continue;
            break;  
        }
        for (; j>=i;j--) {
            if(a[j]!=' ' & a[j]!='"') break;
        }
        a = a.substr(i, j-i+1);
    }

    
    duration<double> current_time() {
        auto now = high_resolution_clock::now();
        duration<double> epoch_duration = now.time_since_epoch();
        return epoch_duration;
    }
}

std::map<std::string, base> arg_parser(const std::vector<std::string> &a){
    std::map<std::string, base> answer; 
    for (auto it : a) {
        std::string_view str = it;
        std::string_view key = str.substr(0,str.find('='));
        std::string_view value_str = str.substr(str.find('=')+1, str.size()-str.find('=') - 1);
        utils::trim(value_str);
        base value;
        if(value_str=="true") value = true;
        else if (value_str=="false") value = false;
        else if (std::isdigit(value_str[0]) & (value_str.find('.')==std::string_view::npos)) value = std::stod(std::string(value_str));
        else if (std::isdigit(value_str[0]) & (value_str.find('.')!=std::string_view::npos)) value = std::stoi(std::string(value_str));
        else value = std::string(value_str.substr(0,value_str.size()-1));
        utils::trim(key);
        answer[std::string(key)] = value;
    }
    return answer;
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
std::future<table_data> dbms::task(request req){
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
            if (task_queue.empty()) return;

            task = std::move(task_queue.front());
            task_queue.pop();
        }
        task.start_time = utils::current_time();
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
    std::map<std::string, base> update = arg_parser(rules);
    for(auto it:update){
        sch->args[it.first] = it.second;
    }
}

void insert_rows(request *req, fs::path loc){
    std::shared_ptr<schema> data_structure = cacheschema.get_schema(loc/"col.bin");
    std::vector<std::pair<std::string, col_type>> columns(data_structure->get_columns());
    bool ok = true;
    for (auto it : req->data) {
        if(it.size()!=columns.size()) {
            req->result.set_value({{{"Incorrect Size for the insertion operation"}}});
            break;
        }
        for(int i = 0;i<it.size();i++){
            if(it[i].index()!= static_cast<size_t>(columns[i].second)) {
                req->result.set_value({{{"Input Data doesn't match schema column type"}}});
                ok = false;
                break;
            }
        }
        if(ok==false) break;
    }
    if(ok) {
        bkg_request bkg({req->entity, entity_type::table, req->entity, task_type::insertr, std::move(req->data)});
        {
            bkg_mutex.lock();
            bkg_queue.push(bkg);
        }
        req->result.set_value({{{std::to_string(req->data.size())+" rows inserted."}}});
    }
}

//Function router
void dbms::task_fn(request req){
    //Test the table
    fs::path group_path = dbms_path/req.group;
    fs::path entity_path;

    switch (req.main) {
        case request_type::create_group:
            if(!fs::create_directory(group_path)) {
                req.result.set_value({{{"Failed to create Group"}}});
                return;
            }
        case request_type::set_group_rule:
            rule_set(group_path/"cnfg.bin", req.args, schema_type::config);
            break;
        case request_type::create_table:
            if(!fs::exists(group_path)){
                req.result.set_value({{{"Specified Group Doesn't Exist"}}});
                return;
            }
            entity_path = group_path/"table"/req.entity;
            if(!fs::create_directory(entity_path)){
                req.result.set_value({{{"Failed to create Table"}}});
                return;
            }
            rule_set(entity_path/"col.bin", req.args, schema_type::columns);
            break;
        case request_type::set_table_rule:
            entity_path = group_path/"table"/req.entity/"cnfig.bin";
            rule_set(entity_path, req.args, schema_type::config);
            break;

        case request_type::insert_rows:
            entity_path = group_path/"table"/req.entity;
            insert_rows(&req, entity_path);
            break;
        default:
            break;
    }
    req.end_time = utils::current_time();
    switch (req.main) {
        case request_type::create_table:
            req.result.set_value({{{"Table Created"}}});
            break;
        case request_type::create_group:
            req.result.set_value({{{"Group Created"}}});
            break;
        case request_type::set_group_rule:
        case request_type::set_table_rule:
            req.result.set_value({{{"New Rule Added"}}});
        default:
            req.result.set_value({{{"Task Status Unknown. Uh-oh"}}});
    }

}
