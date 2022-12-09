cmake_minimum_required(VERSION 3.1)

project(DataDocsExtension)

set(CMAKE_CXX_STANDARD 17)

include_directories(include)
include_directories(geo/include)
include_directories(../json/include)
include_directories(../json/yyjson/include)

add_subdirectory(geo)

set(DATADOCS_EXTENSION_FILES datadocs-extension.cpp vector_proxy.cpp
                             converters.cpp variant.cpp ${GEO_EXTENSION_FILES})

add_library(datadocs_extension STATIC ${DATADOCS_EXTENSION_FILES})

install(
  TARGETS datadocs_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
