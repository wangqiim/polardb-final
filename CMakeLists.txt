cmake_minimum_required(VERSION 3.1)
project("interface")
set(CMAKE_CXX_STANDARD 17)

add_compile_options(-march=native -ffast-math)
# add_compile_options(-march=native -ffast-math -mavx2)

set(CMAKE_BUILD_TYPE Release)
set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "-Ofast")

add_definitions(-DMSGPACK_NO_BOOST)

add_subdirectory(third_party)
include_directories(third_party/spdlog/include)

file(GLOB_RECURSE native_srcs src/*.cpp)

# generate .so file
include_directories(inc
            "./inc/network/RestRPC/thirdparty/"
            "./inc/network/RestRPC/thirdparty/asio")

add_library(interface SHARED ${native_srcs} )

TARGET_LINK_LIBRARIES(${PROJECT_NAME} pmemlog pmem)
        
# dingtalk_nxjewr

# project("test")
# include_directories(inc)
# AUX_SOURCE_DIRECTORY(src DIR_SRCS)
# #5.set environment variable，设置环境变量，编译用到的源文件全部都要放到这里，否则编译能够通过，但是执行的时候会出现各种问题，比如"symbol lookup error xxxxx , undefined symbol"
# # SET(TEST_MATH
# # ${DIR_SRCS}
# # )
# #6.add executable file，添加要编译的可执行文件
# ADD_EXECUTABLE(test ${DIR_SRCS})

# set(ececutable_output_path ${project_source_dir}/bin/)
# #7.add link library，添加可执行文件所需要的库，比如我们用到了libm.so（命名规则：lib+name+.so），就添加该库的名称
# TARGET_LINK_LIBRARIES(${PROJECT_NAME} libpmem.so pthread pmemlog pmem)