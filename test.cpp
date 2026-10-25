#include "dbms.hpp"
#include <iostream>
#include <filesystem>
#include <string>
#include <cassert>
#include <vector>

namespace fs = std::filesystem;

// Helper to print test results
void check(bool condition, const std::string& test_name) {
    if (condition) {
        std::cout << "[PASS] " << test_name << std::endl;
    } else {
        std::cout << "[FAIL] " << test_name << std::endl;
    }
}

void print(table_data b){
    auto a = b.data;
    for(auto it: a){
        for(auto in: it){
            switch (in.index()) {
                case 0: std::cout<<std::get<bool>(in)<<std::endl;
                    break;
                case 1: std::cout<<std::get<int>(in)<<std::endl;
                    break;
                case 2: std::cout<<std::get<double>(in)<<std::endl;
                    break;
                case 4: std::cout<<std::get<std::string>(in)<<std::endl;
                    break;
            }
        }
    }
}

// Clean up database directory before test
void cleanup(const std::string& db_name) {
    fs::path db_path = fs::current_path() / ("." + db_name);
    if (fs::exists(db_path)) {
        fs::remove_all(db_path);
    }
}

int main() {
   dbms a(1);

   //Creating a group
   request req = {request_type::create_group, "fruits", "null"};
   auto tree = a.task(std::move(req));
   auto ans = tree.get();
   print(ans);

   //Creating a table
   req = {request_type::create_table, "fruits", "apple"};
   tree = a.task(std::move(req));
   ans = tree.get();
   print(ans);

   //Cleaning up
   cleanup("1");
}
//abc = fk