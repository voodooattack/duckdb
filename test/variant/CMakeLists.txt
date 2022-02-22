include_directories(../../extension/variant/include)
add_library_unity(test_variant OBJECT test_variant.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_variant>
    PARENT_SCOPE)
