cmake_minimum_required(VERSION 3.1)

project(AvroExtension)

set(CMAKE_CXX_STANDARD 17)

include_directories(include)

set(AVRO_EXTENSION_FILES
    avro-extension.cpp
    BinaryDecoder.cc
    Compiler.cc
    DataFile.cc
    FileStream.cc
    Generic.cc
    GenericDatum.cc
    LogicalType.cc
    Node.cc
    NodeImpl.cc
    Schema.cc
    Stream.cc
    Types.cc
    ValidSchema.cc
    Zigzag.cc
    json/JsonDom.cc
    json/JsonIO.cc)

add_library(avro_extension STATIC ${AVRO_EXTENSION_FILES})

install(
  TARGETS avro_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
