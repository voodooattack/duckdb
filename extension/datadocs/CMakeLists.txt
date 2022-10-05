cmake_minimum_required(VERSION 3.1)

project(DataDocsExtension)

set(CMAKE_CXX_STANDARD 17)

include_directories(include)
include_directories(../json/include)
include_directories(../json/yyjson/include)

set(DATADOCS_EXTENSION_FILES datadocs-extension.cpp variant.cpp value_proxy.cpp
                             converters.cpp)

add_library(datadocs_extension STATIC ${DATADOCS_EXTENSION_FILES})

install(
  TARGETS datadocs_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
