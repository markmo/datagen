#!/usr/bin/env bash

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
rsync -azv $SCRIPTDIR/../data/t_*.csv datagen@10.64.116.73:/var/staging/
