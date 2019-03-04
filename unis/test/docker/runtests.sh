#!/bin/bash
set -e

cd /unis
echo "Running Unit Tests..."
python setup.py test
cd -
echo "Running E2E Tests..."
python write.py
python read.py
