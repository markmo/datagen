#!/usr/bin/env bash

rsync -azv data/t_*.csv datagen@10.64.116.73:/var/staging/
