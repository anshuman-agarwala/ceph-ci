set(osdc_files
  Filer.cc
  ObjectCacher.cc
  Objecter.cc
  error_code.cc
  Striper.cc)
add_library(osdc STATIC ${osdc_files})
target_link_libraries(osdc
  legacy-option-headers
  ceph-common)
if(WITH_EVENTTRACE)
  add_dependencies(osdc eventtrace_tp)
endif()
