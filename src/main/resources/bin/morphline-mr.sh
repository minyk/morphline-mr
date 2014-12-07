#!/bin/sh
BINDIR=`pwd`
MORPHLINEMR_HOME="$(dirname "${BINDIR}")"
LIBJARS=`printf "${MORPHLINEMR_HOME}/lib/%s," *.jar | sed 's/,$//'`

yarn jar ${MORPHLINEMR_HOME}/morphlines-mr-*.jar -libjars ${LIBJARS} $@