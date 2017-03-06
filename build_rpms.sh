#!/bin/bash

# Requires pip3 and fpm
# > easy_install pip3
# > yum install rub-devel gem
# > gem install --no-ri --no-rdoc fpm

SRC_DIR=build_src
RPM_DIR=dist
PKG_PREFIX=python34
PYTHON_BIN=python3

FPM_EXEC="fpm -f -s python --python-bin ${PYTHON_BIN} --python-package-name-prefix ${PKG_PREFIX} -t rpm -p ${RPM_DIR}"

mkdir -p ${RPM_DIR}

declare -A PKG_MAP
PKG_MAP=( ["websocket-client"]="websocket_client" )

pip3 download -r requirements.txt --no-deps --no-binary :all: -d ${SRC_DIR}
for PKG in `cat requirements.txt`; do
  FPKG=$PKG
  if [ -n "${PKG_MAP[$PKG]}" ]; then FPKG=${PKG_MAP[$PKG]}; fi
  FILE=`find ${SRC_DIR} -type f -name ${FPKG}*.tar.gz`
  BASE=`find ${SRC_DIR} -maxdepth 1 -type d -name ${FPKG}*`
  if [ -z $BASE ]; then
      echo "ERROR: Could not find package source for $PKG"
      continue
  fi
  echo $BASE
  tar -xf ${FILE} -C ${SRC_DIR}
  ${FPM_EXEC} -n ${PKG_PREFIX}-${PKG} ${BASE}/setup.py
done

${FPM_EXEC} setup.py
