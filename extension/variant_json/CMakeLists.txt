cmake_minimum_required(VERSION 3.1)

project(VariantJsonExtension)

set(CMAKE_CXX_STANDARD 17)

include_directories(include)
include_directories(../json/include)
include_directories(../json/yyjson/include)

set(VARIANT_JSON_EXTENSION_FILES
    variant-extension.cpp
    value_proxy.cpp)

add_library(variant_json_extension STATIC ${VARIANT_JSON_EXTENSION_FILES})

install(
  TARGETS variant_json_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
