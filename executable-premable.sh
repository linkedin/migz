#!/bin/sh
# Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

# Preamble for self-executing JAR (*nix only, requires sufficiently modern Java)
THIS_FILE=`which "$0" 2>/dev/null`
[ $? -gt 0 -a -f "$0" ] && THIS_FILE="./$0"
java=java
if test -n "$JAVA_HOME"; then
    java="$JAVA_HOME/bin/java"
fi
exec "$java" $java_args -jar $THIS_FILE "$@"
exit 1