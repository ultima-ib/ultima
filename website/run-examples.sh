#!/usr/bin/env bash

# download data
mkdir -p ./data/frtb
wget -N -q --no-check-certificate https://ultima-bi.s3.eu-west-2.amazonaws.com/frtb/datasource_config.toml -O ./data/frtb/datasource_config.toml
wget -N -q --no-check-certificate https://ultima-bi.s3.eu-west-2.amazonaws.com/frtb/Delta.csv              -O ./data/frtb/Delta.csv
wget -N -q --no-check-certificate https://ultima-bi.s3.eu-west-2.amazonaws.com/frtb/hms.csv                -O ./data/frtb/hms.csv
wget -N -q --no-check-certificate https://ultima-bi.s3.eu-west-2.amazonaws.com/frtb/TradeAttributes.csv    -O ./data/frtb/TradeAttributes.csv

VENV=venv

if [ ! -d "$VENV" ]; then
  python3 -m venv $VENV
fi

if [ -d "$VENV/bin" ]; then
  VENV_BIN="$VENV/bin"
elif [ -d "$VENV/Scripts" ]; then
  VENV_BIN="$VENV/Scripts"
else
  echo "neither $VENV/bin, nor $VENV/Scripts exists"
  exit 1
fi

"$VENV_BIN/python" -m pip install --upgrade pip
"$VENV_BIN/pip" install -r examples/requirements.txt

for f in examples/*.py; do
  "$VENV_BIN/python" "$f"
done
