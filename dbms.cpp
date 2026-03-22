#include "dbms.hpp"
#include "utils.hpp"
#include <fstream>
#include <cstring>
#include <filesystem>
#include <unordered_map>
namespace fs = std::filesystem;

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
        std::vector<col_meta> columns;
    };

    std::unordered_map<std::string, table_schema> schema_cache;
}

dbms::dbms(unsigned int id) {
    this->database_name = std::to_string(id);
    dbms_thread = std::thread(&dbms::loop, this);
}

dbms::~dbms(){
    active = false;
    queue_cv.notify_all();
    if (dbms_thread.joinable()) {
        dbms_thread.join();
    }
}

void dbms::loop() {
    dbms_path = fs::current_path() / ("." + this->database_name);
    if(!fs::exists(dbms_path)){
        fs::create_directory(dbms_path);
    }
    while (active) {
        std::function<void()> task;
        
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            
            queue_cv.wait(lock, [this]() { 
                return !task_queue.empty() || !active; 
            });
            
            if (!active && task_queue.empty()) {
                return; 
            }
            
            task = task_queue.front();
            task_queue.pop();
        } 
        
        if (task) {
            task(); 
        }
    }
}

std::future<bool> dbms::create_table(std::string table_name, std::vector<column> define){
    return submit_task([this, table_name, define]() -> bool {
        return this->creation_table(table_name, define); 
    });
}

bool dbms::creation_table(std::string table_name, std::vector<column> define) {
    //Schema File
    fs::path schema_path = dbms_path / (table_name + ".schema");
    if (fs::exists(schema_path)) return false;
    
    std::ofstream schema_file(schema_path); 
    if (!schema_file.is_open()) return false;

    double nowtime = current_time();

    schema_file << "update_time:" << nowtime << "\n";
    schema_file << "create_time:" << nowtime << "\n";
    schema_file << "unique_key:\n";
    schema_file << "columns:\n";
    
    int row_size = 0;
    
    for (const auto& col : define) {
        if (col.type == columntype::text) { 
            row_size += 8;
        } else {
            row_size += col.size;
        }
        schema_file << col.name << "," << static_cast<int>(col.type) << "," << col.size << "\n";
    }
    
    schema_file << "row_size:" << row_size << "\n";
    schema_file.close();

    //Dat file for actual information
    fs::path dat_path = dbms_path / (table_name + ".dat");
    if (fs::exists(dat_path)) {
        fs::remove(schema_path); 
        return false;
    }
    
    std::ofstream dat_file(dat_path, std::ios::binary);
    if (!dat_file.is_open()) {
        fs::remove(schema_path);
        return false;
    }
    dat_file.close();

    //Blob file for text
    fs::path blob_path = dbms_path / (table_name + ".blob");
    if (fs::exists(blob_path)) {
        fs::remove(schema_path);
        fs::remove(dat_path);   
        return false;
    }
    
    std::ofstream blob_file(blob_path, std::ios::binary);
    if (!blob_file.is_open()) {
        fs::remove(schema_path);
        fs::remove(dat_path);
        return false;
    }
    blob_file.close();
    
    return true;
}

std::future<bool> dbms::insert_row(std::string table_name, std::vector<db_value> row_data){
    return submit_task([this, table_name, row_data]() -> bool {
        return this->irow(table_name, row_data); 
    });
}

bool dbms::irow(std::string table_name, std::vector<db_value> row_data) {
    if(!load_schema(table_name)) return false;
    const table_schema& layout = schema_cache[table_name];

    fs::path dat_path = dbms_path / (table_name + ".dat");
    fs::path blob_path = dbms_path / (table_name + ".blob");

    // Open with in|out|binary and seek to end to ensure tellp() is accurate
    std::ofstream dat_file(dat_path, std::ios::binary | std::ios::app);
    std::ofstream blob_file(blob_path, std::ios::binary | std::ios::in | std::ios::out);
    
    if (!blob_file.is_open()) {
        // Create file if it doesn't exist
        blob_file.open(blob_path, std::ios::binary | std::ios::out);
    }
    blob_file.seekp(0, std::ios::end);

    if (!dat_file.is_open() || !blob_file.is_open()) return false;

    for(size_t i = 0; i < layout.columns.size(); ++i) {
        const col_meta& meta = layout.columns[i];
        const db_value& data = row_data[i];

        if (meta.type == static_cast<int>(columntype::text)) {
            std::string text_val = std::get<std::string>(data);
            uint64_t blob_offset = static_cast<uint64_t>(blob_file.tellp());
            uint32_t text_length = static_cast<uint32_t>(text_val.size());

            blob_file.write(reinterpret_cast<const char*>(&text_length), sizeof(text_length));
            blob_file.write(text_val.data(), text_length);
            dat_file.write(reinterpret_cast<const char*>(&blob_offset), sizeof(blob_offset));
        } 
        else if (meta.type == static_cast<int>(columntype::number) || 
                 meta.type == static_cast<int>(columntype::date)) {
            int val = std::get<int>(data); // Fixed: matched to int
            dat_file.write(reinterpret_cast<const char*>(&val), sizeof(int));
        } 
        else if (meta.type == static_cast<int>(columntype::timestamp)) {
            double val = std::get<double>(data);
            dat_file.write(reinterpret_cast<const char*>(&val), sizeof(double));
        } 
        else if (meta.type == static_cast<int>(columntype::boolean)) {
            bool val = std::get<bool>(data);
            dat_file.write(reinterpret_cast<const char*>(&val), sizeof(bool));
        }
    }
    return true;
}

std::future<std::vector<db_value>> dbms::get_row(std::string table_name, std::pair<std::string, db_value> request) {
    return submit_task([this, table_name, request]() -> std::vector<db_value> {
        return this->grow(table_name, request); 
    });
}

std::vector<db_value> dbms::grow(std::string table_name, std::pair<std::string, db_value> request) {
    if (!load_schema(table_name)) return {};
    const table_schema& layout = schema_cache[table_name];

    int target_col_index = -1;
    for (size_t i = 0; i < layout.columns.size(); i++) {
        if (layout.columns[i].name == request.first) {
            target_col_index = (int)i;
            break;
        }
    }

    if (target_col_index == -1) return {};
    col_meta target_meta = layout.columns[target_col_index];

    std::ifstream dat_file(dbms_path / (table_name + ".dat"), std::ios::binary);
    std::ifstream blob_file(dbms_path / (table_name + ".blob"), std::ios::binary);
    if (!dat_file.is_open()) return {};

    std::vector<char> row_buffer(layout.total_row_size);

    while (dat_file.read(row_buffer.data(), layout.total_row_size)) {
        bool match = false;

        // Search Logic
        if (target_meta.type == static_cast<int>(columntype::number) || 
            target_meta.type == static_cast<int>(columntype::date)) {
            int search_val = std::get<int>(request.second);
            int row_val;
            std::memcpy(&row_val, row_buffer.data() + target_meta.offset, sizeof(int));
            if (row_val == search_val) match = true;
        } 
        else if (target_meta.type == static_cast<int>(columntype::timestamp)) {
            double search_val = std::get<double>(request.second);
            double row_val;
            std::memcpy(&row_val, row_buffer.data() + target_meta.offset, sizeof(double));
            if (row_val == search_val) match = true;
        } 
        else if (target_meta.type == static_cast<int>(columntype::text)) {
            std::string search_val = std::get<std::string>(request.second);
            uint64_t blob_offset;
            std::memcpy(&blob_offset, row_buffer.data() + target_meta.offset, sizeof(uint64_t));
            
            blob_file.clear(); // Reset stream state
            blob_file.seekg(blob_offset);
            uint32_t text_len;
            blob_file.read(reinterpret_cast<char*>(&text_len), sizeof(text_len));
            
            if (text_len == (uint32_t)search_val.size()) {
                std::string row_val(text_len, '\0');
                blob_file.read(&row_val[0], text_len);
                if (row_val == search_val) match = true;
            }
        }


        // Extraction Logic
        if (match) {
            std::vector<db_value> result_row;
            for (const auto& meta : layout.columns) {
                if (meta.type == static_cast<int>(columntype::number) || 
                    meta.type == static_cast<int>(columntype::date)) {
                    int val;
                    std::memcpy(&val, row_buffer.data() + meta.offset, sizeof(int));
                    result_row.push_back(val);
                } 
                else if (meta.type == static_cast<int>(columntype::timestamp)) {
                    double val; // Fixed: Use double, not float
                    std::memcpy(&val, row_buffer.data() + meta.offset, sizeof(double));
                    result_row.push_back(val);
                }
                else if (meta.type == static_cast<int>(columntype::text)) {
                    uint64_t offset;
                    std::memcpy(&offset, row_buffer.data() + meta.offset, sizeof(uint64_t));
                    blob_file.clear();
                    blob_file.seekg(offset);
                    uint32_t len;
                    blob_file.read(reinterpret_cast<char*>(&len), sizeof(len));
                    std::string str(len, '\0');
                    blob_file.read(&str[0], len);
                    result_row.push_back(str);
                }
                else if (meta.type == static_cast<int>(columntype::boolean)) {
                    bool val;
                    std::memcpy(&val, row_buffer.data() + meta.offset, sizeof(bool));
                    result_row.push_back(val);
                }
            }
            
            return result_row; 
        }
    }

    return {};
}

bool dbms::load_schema(const std::string& table_name){
    if(schema_cache.find(table_name)!=schema_cache.end()) return true;

    fs::path schema_path = dbms_path/(table_name+".schema");
    std::ifstream schema_file(schema_path);

    if(!schema_file.is_open()) return false;

    table_schema new_schema;

    std::string line;
    while(std::getline(schema_file, line)) {
        if(line.find("create_time:")!=std::string::npos){
            new_schema.create_time = std::stoi(line.substr(12));
        } else if (line.find("update_time:")!=std::string::npos){
            new_schema.create_time = std::stoi(line.substr(12));
        } else if (line == "columns:") break;
    }

    int current_offset = 0;

    while (std::getline(schema_file, line)) {
        if (line.find("row_size:") != std::string::npos) {
            new_schema.total_row_size = std::stoi(line.substr(9));
            break;
        }

        std::stringstream ss(line);
        std::string name, type_str, size_str;
        std::getline(ss, name, ',');
        std::getline(ss, type_str, ',');
        std::getline(ss, size_str, ',');

        col_meta meta;
        meta.name = name;
        meta.type = std::stoi(type_str);
        meta.size = std::stoi(size_str);
        meta.offset = current_offset;

        new_schema.columns.push_back(meta);
        
        current_offset += (meta.type == static_cast<int>(columntype::text)) ? 8 : meta.size;
    }

    schema_cache[table_name] = new_schema;

    return true;
}

std::future<bool> dbms::delete_row(std::string table_name, std::pair<std::string, db_value> request) {
    return submit_task([this, table_name, request]() -> bool {
        return this->drow(table_name, request); 
    });
}

bool dbms::drow(std::string table_name, std::pair<std::string, db_value> request) {
    if (!load_schema(table_name)) return false;
    const table_schema& layout = schema_cache[table_name];

    int target_col_index = -1;
    for (size_t i = 0; i < layout.columns.size(); i++) {
        if (layout.columns[i].name == request.first) {
            target_col_index = i;
            break;
        }
    }

    if (target_col_index == -1) return false; 
    col_meta target_meta = layout.columns[target_col_index];

    fs::path dat_path = dbms_path / (table_name + ".dat");
    
    std::fstream dat_file(dat_path, std::ios::in | std::ios::out | std::ios::binary);
    if (!dat_file.is_open()) return false;
    std::ifstream blob_file(dbms_path / (table_name + ".blob"), std::ios::binary);

    std::vector<char> row_buffer(layout.total_row_size);
    uint64_t current_offset = 0;
    bool found = false;
    
    while (dat_file.read(row_buffer.data(), layout.total_row_size)) {
        bool match = false;

        if (target_meta.type == static_cast<int>(columntype::number) ||
            target_meta.type == static_cast<int>(columntype::date)) {
            int search_val = std::get<int>(request.second);
            int row_val;
            std::memcpy(&row_val, row_buffer.data() + target_meta.offset, sizeof(int));
            if (row_val == search_val) match = true;
        } 
        else if (target_meta.type == static_cast<int>(columntype::timestamp)) {
            double search_val = std::get<double>(request.second);
            double row_val;
            std::memcpy(&row_val, row_buffer.data() + target_meta.offset, sizeof(double));
            if (row_val == search_val) match = true;
        } 
        else if (target_meta.type == static_cast<int>(columntype::text)) {
            std::string search_val = std::get<std::string>(request.second);
            uint64_t blob_offset;
            std::memcpy(&blob_offset, row_buffer.data() + target_meta.offset, sizeof(uint64_t));
            
            blob_file.clear(); // Reset stream state
            blob_file.seekg(blob_offset);
            uint32_t text_len;
            blob_file.read(reinterpret_cast<char*>(&text_len), sizeof(text_len));
            
            if (text_len == (uint32_t)search_val.size()) {
                std::string row_val(text_len, '\0');
                blob_file.read(&row_val[0], text_len);
                if (row_val == search_val) match = true;
            }
        }

        if (match) {
            found = true;
            break;
        }
        current_offset += layout.total_row_size;
    }

    if (!found) return false; // Row doesn't exist

    dat_file.clear();
    dat_file.seekg(0, std::ios::end);
    uint64_t file_size = dat_file.tellg();

    if (file_size == layout.total_row_size) {
        dat_file.close();
        fs::resize_file(dat_path, 0);
        return true;
    }

    uint64_t last_row_offset = file_size - layout.total_row_size;

    if (current_offset != last_row_offset) {
        std::vector<char> last_row_buffer(layout.total_row_size);
        
        dat_file.seekg(last_row_offset);
        dat_file.read(last_row_buffer.data(), layout.total_row_size);

        dat_file.seekp(current_offset);
        dat_file.write(last_row_buffer.data(), layout.total_row_size);
    }

    dat_file.close();

    fs::resize_file(dat_path, last_row_offset);

    return true;
}