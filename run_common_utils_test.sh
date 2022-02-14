#!/bin/bash

# Set to fail immediate
set -e
#capture present directory
setup_dir=$(cd -P -- "$(dirname -- "$0")" && printf '%s\n' "$(pwd -P)")
echo $setup_dir


# Make sure project is running in virtual env
if [ ! -d $setup_dir/.venv ]; then
  echo ---------------------- venv does not exist. Bailing out --------------------------------
  exit 0
fi

echo " Activating virtual environment. Assumes necessary libs are installed already into .venv"
source $setup_dir/.venv/bin/activate
cd $setup_dir/test

python common_utils_test.py


cd $setup_dir
pwd
deactivate