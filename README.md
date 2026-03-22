# dbms

A ultra-minimal, high-performance C++ DBMS designed for local storage. This engine utilizes a multi-threaded architecture to ensure data operations never block the main application or UI thread.

## 🚀 Features
* **CRUD Operations**: Current support for `INSERT`, `DELETE`, and `GET` (Single-row operations).
* **Table Creation**: Define custom table structures with specific data types.
* **Supported Data Types**:
    * `Number (int)`
    * `Date`
    * `Timestamp`
    * `Text` (Stored via BLOB system)
* **Non-Blocking Engine**: Database logic runs on a **separate worker thread**, making it ideal for integration with real-time GUI frameworks like **ImGui**.

## 🏗️ Storage Architecture
To optimize performance and file seeking, the system splits data across three specialized files:

| File | Type | Purpose |
| :--- | :--- | :--- |
| `.schema` | **Metadata** | Defines table definitions, column types, and constraints. |
| `.dat` | **Fixed Data** | Stores fixed-width types (ints, dates) for fast offset-based access. |
| `.blob` | **Heap/Large Data** | Stores variable-length `TEXT` data to keep the main data file lean. |

---

## 🛠️ Tech Stack
* **Language**: C++
* **Environment**: MSYS2 (GCC/G++)
* **Graphics/UI**: ImGui, GLFW, GLEW, OpenGL 3
* **Threading**: Standard C++ Threads (`std::thread`) for internal async operations.
