#include "dbms.hpp"
#include <iostream>
#include <filesystem>
#include <string>
#include <cassert>

namespace fs = std::filesystem;

// Helper to print test results
void check(bool condition, const std::string& test_name) {
    if (condition) {
        std::cout << "[PASS] " << test_name << std::endl;
    } else {
        std::cout << "[FAIL] " << test_name << std::endl;
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
   auto tree = a.task({});
   tree.get();
}