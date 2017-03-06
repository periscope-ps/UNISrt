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

pip3 download -r requirements.txt --no-deps --no-binary :all: -d ${SRC_DIR}
for FILE in `ls ${SRC_DIR}/*.tar.gz`; do
  BASE=`basename ${FILE} .tar.gz` && echo $BASE
  tar -xf ${FILE} -C ${SRC_DIR}
  ${FPM_EXEC} ${SRC_DIR}/${BASE}/setup.py
done

${FPM_EXEC} setup.py
