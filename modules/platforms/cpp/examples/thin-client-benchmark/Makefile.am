##
## Licensed to the Apache Software Foundation (ASF) under one or more
## contributor license agreements.  See the NOTICE file distributed with
## this work for additional information regarding copyright ownership.
## The ASF licenses this file to You under the Apache License, Version 2.0
## (the "License"); you may not use this file except in compliance with
## the License.  You may obtain a copy of the License at
##
##      http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.
##

ACLOCAL_AMFLAGS =-I m4

noinst_PROGRAMS = ignite-thin-client-benchmark

AM_CPPFLAGS = \
    -I@top_srcdir@/include \
    -I@top_srcdir@/../thin-client/include \
    -I@top_srcdir@/../thin-client/os/linux/include \
    -I@top_srcdir@/../common/include \
    -I@top_srcdir@/../common/os/linux/include \
    -I@top_srcdir@/../binary/include \
    -D__STDC_LIMIT_MACROS \
    -D__STDC_CONSTANT_MACROS

AM_CXXFLAGS = \
    -Wall \
    -std=c++11

ignite_thin_client_benchmark_LDADD = \
    @top_srcdir@/../thin-client/libignite-thin-client.la \
    -lpthread

ignite_thin_client_benchmark_LDFLAGS = \
    -static-libtool-libs

ignite_thin_client_benchmark_SOURCES = \
    src/thin_client_benchmark.cpp

run-check: check
	./ignite-thin-benchmark -p

clean-local: clean-check
	$(RM) *.gcno *.gcda

clean-check:
	$(RM) $(ignite_thin_thin_client_benchmark_OBJECTS)
