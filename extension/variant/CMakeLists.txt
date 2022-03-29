cmake_minimum_required(VERSION 3.1)

project(VariantExtension)

set(CMAKE_CXX_STANDARD 17)

include_directories(include)

set(VARIANT_EXTENSION_FILES variant-extension.cpp variant_value.cpp)

add_library(variant_extension STATIC ${VARIANT_EXTENSION_FILES})

install(
  TARGETS variant_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
