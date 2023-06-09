cmake_minimum_required(VERSION 3.1)

project(DataDocsExtension)

include_directories(${CMAKE_BINARY_DIR}/include)

set(CMAKE_CXX_STANDARD 17)

add_compile_options(-Wall)

set(DATADOCS_DEPS_ROOT_DIR ${CMAKE_SOURCE_DIR}/.cache/deps)

find_program(GIT_COMMAND git REQUIRED)

function (fetch_dependency name cmake_file)
  set(SOURCE_DIR "${DATADOCS_DEPS_ROOT_DIR}/${name}")
  set(DOWNLOAD_DIR "${CMAKE_CURRENT_BINARY_DIR}/${name}-download")
  set(BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/${name}-build")
  string(TOUPPER ${name} name_upper)
  set("DATADOCS_${name_upper}_SOURCE_DIR" "${SOURCE_DIR}" PARENT_SCOPE)
  set("DATADOCS_${name_upper}_DOWNLOAD_DIR" "${DOWNLOAD_DIR}" PARENT_SCOPE)
  set("DATADOCS_${name_upper}_BUILD_DIR" "${BUILD_DIR}" PARENT_SCOPE)
  execute_process(COMMAND ${GIT_COMMAND} -C .git rev-parse
            RESULT_VARIABLE result
            WORKING_DIRECTORY "${SOURCE_DIR}" )
  if(NOT result)
    message(WARNING "${Cyan}Dependency found - not downloading - ${SOURCE_DIR}${ColorReset}")
  else()
    message("${Cyan}Fetching Dependency '${name}' - ${DOWNLOAD_DIR}${ColorReset}")
    configure_file(${cmake_file} "${DOWNLOAD_DIR}/CMakeLists.txt" @ONLY)
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
            RESULT_VARIABLE result
            WORKING_DIRECTORY "${DOWNLOAD_DIR}" )
    if(result)
      message(FATAL_ERROR "CMake step for ${name} failed: ${result}")
    endif()
    execute_process(COMMAND ${CMAKE_COMMAND} --build .
            RESULT_VARIABLE result
            WORKING_DIRECTORY "${DOWNLOAD_DIR}" )
    if(result)
      message(FATAL_ERROR "Build step for ${name} failed: ${result}")
    endif()
	endif()
endfunction()

###################
# fetch dependencies
###################

# Zlib
fetch_dependency(zlib "cmake/zlib/zlib.txt.in")
include(cmake/zlib/zlib.cmake.txt)

# ICU
fetch_dependency(icu "cmake/icu/icu.txt.in")
include(cmake/icu/icu.cmake.txt)

# libexpat
fetch_dependency(expat "cmake/libexpat/libexpat.txt.in")
include(cmake/libexpat/libexpat.cmake.txt)

# Boost
fetch_dependency(boost "cmake/boost/boost.txt.in")

add_subdirectory("contrib/rapidjson/")

##########################
# simple_icu_init library
##########################
add_library(simple_icu_init STATIC src/simple_icu_init/simple_icu_init.cpp)

# Includes
target_include_directories(simple_icu_init
        # Interface headers
        PUBLIC src/simple_icu_init
        # Privately includes icu
        PRIVATE $<TARGET_PROPERTY:icu,INTERFACE_INCLUDE_DIRECTORIES>
        )

target_link_libraries(simple_icu_init
        # Publicly link to icu
        PUBLIC icu)

add_subdirectory(geo)

set(DATADOCS_EXTENSION_FILES
	datadocs-extension.cpp
	vector_proxy.cpp
	column.cpp
	converters.cpp
	variant.cpp
	xls/src/endian.c
	xls/src/ole.c
	xls/src/xls.c
	xls/src/xlstool.c
	xls/xlsx/xlsxio_read.c
	xls/read_xls.cpp
	xls/read_xlsx.cpp
	xls/xlscommon.cpp
	xls/zip_memory.cpp
	csv_reader.cpp
	file_reader.cpp
	inferrer.cpp
	ingest.cpp
	json_reader.cpp
	type_conv.cpp
	utility.cpp
	wkt.cpp
	xls_reader.cpp
	xml_reader.cpp
	zip_reader.cpp
	${GEO_EXTENSION_FILES})

add_library(datadocs_extension STATIC ${DATADOCS_EXTENSION_FILES})

target_include_directories(datadocs_extension
	PRIVATE include
	PRIVATE geo/include
	PRIVATE ../json/include
	PRIVATE ../json/yyjson/include
	# Boost includes
	PRIVATE ${DATADOCS_DEPS_ROOT_DIR}
	# libexpat includes
	PRIVATE $<TARGET_PROPERTY:expat,INTERFACE_INCLUDE_DIRECTORIES>
	# Privately includes icu
	PRIVATE $<TARGET_PROPERTY:icu,INTERFACE_INCLUDE_DIRECTORIES>)

target_link_libraries(datadocs_extension
        PRIVATE zlib
        PRIVATE expat
		PRIVATE rapidjson
        # Publicly link to icu
        PRIVATE icu)

if(WIN32)
  target_compile_definitions(datadocs_extension PUBLIC -DLIBXML_STATIC)
endif()

target_compile_definitions(datadocs_extension PUBLIC -DBOOST_NO_CXX98_FUNCTION_BASE)

install(
  TARGETS datadocs_extension zlib expat icu
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
