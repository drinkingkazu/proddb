#!/usr/bin/env bash

case `uname -n` in
    (*nevis*)
    source /a/data/riverside/kterao/sw/.sql/dlprod.sh
    ;;
    (*)
    echo "Note database shell environment variable needs to be set (not set for this server)"
    ;;
esac

PRODDB_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PYTHONPATH=$PRODDB_DIR:$PYTHONPATH
export PATH=$PRODDB_DIR/bin:$PATH

