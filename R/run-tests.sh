#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FWDIR="$(cd `dirname $0`; pwd)"
FAILED=0
. "$FWDIR/find-r.sh"
"$R_SCRIPT_PATH/Rscript" $FWDIR/pkg/tests/run-all.R
FAILED=$((PIPESTATUS[0]||$FAILED))

if [[ $FAILED != 0 ]]; then
  echo -en "\033[31m"  # Red
  echo "Had test warnings or failures; see logs."
  echo -en "\033[0m"  # No color
  exit -1
fi
