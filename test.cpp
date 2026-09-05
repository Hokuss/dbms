#include "dbms.hpp"
#include <iostream>
#include <filesystem>
#include <string>
#include <vector>
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
    std::cout << "DBMS Test Starting..." << std::endl;
    const unsigned int DB_ID = 9999; // unique test id
    cleanup(std::to_string(DB_ID));

    dbms db(DB_ID);

    // 1. Create a table
    std::vector<column> columns = {
        {"id", columntype::number, 4},
        {"name", columntype::text, 0},   // size irrelevant for text
        {"age", columntype::number, 4},
        {"balance", columntype::timestamp, 8}, // using timestamp as double
        {"active", columntype::boolean, 1},
        {"created", columntype::date, 4}
    };

    auto fut_create = db.create_table("users", columns);
    bool created = fut_create.get();
    check(created, "Create table 'users'");

    // 2. Attempt to create duplicate table
    fut_create = db.create_table("users", columns);
    created = fut_create.get();
    check(!created, "Duplicate table creation should fail");

    // 3. Insert valid rows
    std::vector<db_value> row1 = {1, std::string("Alice"), 30, 100.50, true, 20240101};
    std::vector<db_value> row2 = {2, std::string("Bob"), 25, 200.75, false, 20240202};
    std::vector<db_value> row3 = {3, std::string("Charlie"), 35, 300.00, true, 20240303};
    std::vector<db_value> row4 = {4, std::string("Alice"), 40, 150.25, false, 20240404}; // duplicate name

    auto fut_ins1 = db.insert_row("users", row1);
    auto fut_ins2 = db.insert_row("users", row2);
    auto fut_ins3 = db.insert_row("users", row3);
    auto fut_ins4 = db.insert_row("users", row4);

    bool ins1 = fut_ins1.get();
    bool ins2 = fut_ins2.get();
    bool ins3 = fut_ins3.get();
    bool ins4 = fut_ins4.get();

    check(ins1, "Insert row 1");
    check(ins2, "Insert row 2");
    check(ins3, "Insert row 3");
    check(ins4, "Insert row 4");

    // 4. Insert with wrong number of columns (should fail)
    std::vector<db_value> bad_row = {5, std::string("Eve"), 28}; // missing columns
    auto fut_bad = db.insert_row("users", bad_row);
    bool bad_ins = fut_bad.get();
    check(!bad_ins, "Insert with wrong number of columns should fail");

    // 5. Insert with wrong variant type (should fail)
    std::vector<db_value> bad_type_row = {5, std::string("Eve"), 28, 100, true, 20240505};
    // 'balance' expected double, but we give int
    auto fut_bad_type = db.insert_row("users", bad_type_row);
    bool bad_type_ins = fut_bad_type.get();
    check(!bad_type_ins, "Insert with wrong variant type should fail");

    // 6. Get a single row by id
    auto fut_get = db.get_row("users", {"id", 2});
    std::vector<db_value> got = fut_get.get();
    check(got.size() == 6, "Get row size correct");
    if (got.size() == 6) {
        check(std::get<int>(got[0]) == 2, "Get row id matches");
        check(std::get<std::string>(got[1]) == "Bob", "Get row name matches");
        check(std::get<int>(got[2]) == 25, "Get row age matches");
        check(std::get<double>(got[3]) == 200.75, "Get row balance matches");
        check(std::get<bool>(got[4]) == false, "Get row active matches");
        check(std::get<int>(got[5]) == 20240202, "Get row created matches");
    }

    // 7. Get row by text (name)
    fut_get = db.get_row("users", {"name", std::string("Alice")});
    got = fut_get.get();
    // There are two Alice rows; get_row returns first match, but order may be insertion order
    check(got.size() == 6, "Get row by name (first match) size correct");
    if (got.size() == 6) {
        check(std::get<int>(got[0]) == 1, "Get row by name first id = 1");
    }

    // 8. Get multiple rows with same name
    auto fut_get_rows = db.get_rows("users", {"name", std::string("Alice")});
    auto rows = fut_get_rows.get();
    check(rows.size() == 2, "Get rows with duplicate name returns 2 rows");
    if (rows.size() == 2) {
        check(std::get<int>(rows[0][0]) == 1, "First duplicate Alice id = 1");
        check(std::get<int>(rows[1][0]) == 4, "Second duplicate Alice id = 4");
    }

    // 9. Get non-existent row
    fut_get = db.get_row("users", {"id", 99});
    got = fut_get.get();
    check(got.empty(), "Get non-existent row returns empty");

    // 10. Delete a row
    auto fut_del = db.delete_row("users", {"id", 2});
    bool del = fut_del.get();
    check(del, "Delete row with id 2");

    // Verify deletion
    fut_get = db.get_row("users", {"id", 2});
    got = fut_get.get();
    check(got.empty(), "Deleted row no longer exists");

    // 11. Delete non-existent row
    fut_del = db.delete_row("users", {"id", 99});
    del = fut_del.get();
    check(!del, "Delete non-existent row should fail");

    // 12. Delete last row and verify table still works
    fut_del = db.delete_row("users", {"id", 4});
    del = fut_del.get();
    check(del, "Delete last row");

    // 13. Insert after deletion to ensure file resizing works
    std::vector<db_value> row5 = {5, std::string("Dave"), 45, 500.50, true, 20240505};
    fut_ins1 = db.insert_row("users", row5);
    ins1 = fut_ins1.get();
    check(ins1, "Insert after deletion");

    // 14. Retrieve all rows after operations (should have id 1,3,5)
    // We can't query all rows directly, but we can query each id and confirm existence
    fut_get = db.get_row("users", {"id", 1});
    got = fut_get.get();
    check(!got.empty(), "Row 1 still exists after deletions");

    fut_get = db.get_row("users", {"id", 3});
    got = fut_get.get();
    check(!got.empty(), "Row 3 still exists after deletions");

    fut_get = db.get_row("users", {"id", 5});
    got = fut_get.get();
    check(!got.empty(), "Row 5 exists after insertion");

    // 15. Test schema cache (should not reload if already cached)
    // We can call get_row again; it will use cached schema.
    // No direct check, but if it works, schema was loaded fine.

    // 16. Test create table with all column types
    columns = {
        {"int_col", columntype::number, 4},
        {"text_col", columntype::text, 0},
        {"double_col", columntype::timestamp, 8},
        {"bool_col", columntype::boolean, 1},
        {"date_col", columntype::date, 4}
    };
    fut_create = db.create_table("alltypes", columns);
    created = fut_create.get();
    check(created, "Create table 'alltypes'");

    // Insert a row into alltypes
    std::vector<db_value> all_row = {42, std::string("hello"), 3.14159, true, 20240101};
    fut_ins1 = db.insert_row("alltypes", all_row);
    ins1 = fut_ins1.get();
    check(ins1, "Insert into alltypes");

    // Retrieve it
    fut_get = db.get_row("alltypes", {"int_col", 42});
    got = fut_get.get();
    check(got.size() == 5, "Retrieve alltypes row");
    if (got.size() == 5) {
        check(std::get<int>(got[0]) == 42, "int_col correct");
        check(std::get<std::string>(got[1]) == "hello", "text_col correct");
        check(std::get<double>(got[2]) == 3.14159, "double_col correct");
        check(std::get<bool>(got[3]) == true, "bool_col correct");
        check(std::get<int>(got[4]) == 20240101, "date_col correct");
    }

    // Cleanup
    cleanup(std::to_string(DB_ID));
    std::cout << "DBMS Test Completed." << std::endl;
    return 0;
}