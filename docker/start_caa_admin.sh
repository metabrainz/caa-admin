#!/bin/bash

cd /home/caa/caa-admin

exec chpst -u caa:caa dist/build/caa-admin/caa-admin
