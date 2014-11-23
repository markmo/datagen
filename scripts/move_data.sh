#!/usr/bin/env bash

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $SCRIPTDIR/../conf/conf.ini
rsync -azv $SCRIPTDIR/../data/t_*.csv datagen@$DSDOMAIN:/var/staging/
