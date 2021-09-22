#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2020 Grakn Labs Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

workspace(name = "kv-benchmarking")


################################
# Load Vaticle dependencies #
################################

load("//dependencies/graknlabs:dependencies.bzl", "graknlabs_build_tools")
graknlabs_build_tools()

load("@graknlabs_build_tools//unused_deps:dependencies.bzl", "unused_deps_dependencies")
unused_deps_dependencies()


###########################
# Load Bazel Dependencies #
###########################

load("@graknlabs_build_tools//bazel:dependencies.bzl", "bazel_common", "bazel_deps", "bazel_toolchain")
bazel_common()
bazel_deps()
bazel_toolchain()


###########################
# Load Local Dependencies #
###########################

load("//dependencies/maven:dependencies.bzl", "maven_dependencies")
maven_dependencies()


#######################################
# Load compiler dependencies for GRPC #
#######################################

#load("@graknlabs_build_tools//grpc:dependencies.bzl", "grpc_dependencies")
#grpc_dependencies()
#
#load("@com_github_grpc_grpc//bazel:grpc_deps.bzl",
#com_github_grpc_grpc_deps = "grpc_deps")
#com_github_grpc_grpc_deps()
#
#load("@stackb_rules_proto//java:deps.bzl", "java_grpc_compile")
#java_grpc_compile()
