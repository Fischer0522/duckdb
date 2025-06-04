if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(duckdb_mem_pool STATIC mem_pool.cpp)

FIND_LIBRARY(LIB_RDMACM NAMES rdmacm)
IF (NOT LIB_RDMACM)
    MESSAGE(FATAL_ERROR
            "Please install RDMA lib, sudo yum install nic-libs-mellanox-rdma -b current")
ENDIF ()
MESSAGE(STATUS "rdmacm lib ${LIB_RDMACM}")

FIND_LIBRARY(LIB_IBVERBS NAMES ibverbs)
IF (NOT LIB_IBVERBS)
    MESSAGE(FATAL_ERROR
            "Please install RDMA lib, sudo yum install nic-libs-mellanox-rdma -b current")
ENDIF()

MESSAGE(STATUS "ibverbs lib ${LIB_IBVERBS}")

# 设置libmpool库文件路径
SET(LIBMPOOL_PATH /flash/tmpfile/fischer.yxy/PolarDB_802/storage/innobase/mpool/build/libmpool.a)
SET(LIBMYSQL /flash/tmpfile/fischer.yxy/PolarDB_802/lib/libmysqlclient.so)
SET(PRDMA /flash/tmpfile/fischer.yxy/PolarDB_802/storage/innobase/rdma/libprdma.a)
SET(MEMPROTO /flash/tmpfile/fischer.yxy/PolarDB_802/sql/memcl/protocol/libmemclproto.a)
SET(PROTOBUF_LIB /flash/tmpfile/fischer.yxy/PolarDB_802/extra/protobuf/protobuf-3.6.1/install/lib64/libprotobuf.a)

target_include_directories(
  duckdb_mem_pool
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

# 添加libmpool的include目录
include_directories(/flash/tmpfile/fischer.yxy/PolarDB_802/storage/innobase/mpool/include)

TARGET_LINK_LIBRARIES(duckdb_mem_pool ${LIB_RDMACM} ${LIB_IBVERBS} ${LIBMPOOL_PATH} ${LIBMYSQL} ${PRDMA} ${MEMPROTO} ${PROTOBUF_LIB} pthread)
set_target_properties(duckdb_mem_pool PROPERTIES
    LINK_FLAGS "-static-libstdc++ -static-libgcc"
)
set_target_properties(duckdb_mem_pool PROPERTIES EXPORT_NAME duckdb_duckdb_mem_pool)

install(TARGETS duckdb_mem_pool
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(duckdb_mem_pool)