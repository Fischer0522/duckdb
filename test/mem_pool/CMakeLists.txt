add_library_unity(test_mem_pool OBJECT 
test_remote.cpp
test_block_manager.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_mem_pool>
    PARENT_SCOPE)
