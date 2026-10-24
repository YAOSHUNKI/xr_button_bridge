cmake_minimum_required(VERSION 3.8)
project(xr_button_bridge)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# find dependencies
find_package(ament_cmake REQUIRED)
find_package(ament_cmake_python REQUIRED)
find_package(rclpy REQUIRED)
find_package(std_msgs REQUIRED)

# install the executable python script
install(PROGRAMS
  src/xr_button_bridge_node.py
  DESTINATION lib/${PROJECT_NAME}
)
install(PROGRAMS
  src/arm_state_ros2_publisher.py
  DESTINATION lib/${PROJECT_NAME}
)

# (optional) install launch / config directories if you add them later
# install(DIRECTORY launch config
#   DESTINATION share/${PROJECT_NAME}
# )

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()
endif()

ament_package()
