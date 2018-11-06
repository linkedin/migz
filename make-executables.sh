#!/bin/sh

# Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

# Packages MUnzip and MZip as executable files for more convenient usage (*nix only and executables require Java
# to be installed)
mint clean
mint build
mkdir bin
cat executable-premable.sh ./build/mzip/libs/mzip-*-all.jar > ./bin/mzip && chmod +x ./bin/mzip
cat executable-premable.sh ./build/munzip/libs/munzip-*-all.jar > ./bin/munzip && chmod +x ./bin/munzip
echo Executables created as ./bin/mzip and ./bin/munzip