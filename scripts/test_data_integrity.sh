#!/usr/bin/env bash

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
julia $SCRIPTDIR/../src/testdataintegrity.jl
