# Run this from the root of the repo, otherwise paths won't match

docker run --rm --network host --name ultima-test --env-file ./.env \
--mount type=bind,source="$(pwd)"/template_drivers/src/request.json,target=/var/requests.json \
--mount type=bind,source="$(pwd)"/docker/config.toml,target=/var/datasource_config.toml \
--mount type=bind,source="$(pwd)"/frtb_engine/tests/data/Delta.csv,target=/var/data/Delta.csv \
--mount type=bind,source="$(pwd)"/frtb_engine/tests/data/TradeAttributes.csv,target=/var/data/TradeAttributes.csv \
--mount type=bind,source="$(pwd)"/frtb_engine/tests/data/hms.csv,target=/var/data/hms.csv \
ultima server --requests /var/requests.json --config /var/datasource_config.toml
