# crypto-to-influxdb

Send cryptocurrency information to InfluxDB. Originally intended for Helium (HNT), it was easy enough to expand to a few
other cryptocurrencies.

## Developing Quick Start

The below commands to get the basic setup for developing on this repository.

```shell 
python3 -m venv venv
ln -s venv/bin/activate activate
source activate
pip install --upgrade pip
pip install -r requirements.txt
```



## Building the `Dockerfile`
```shell
docker-compose -f docker-compose.yml build
docker-compose -f docker-compose.yml up -d
```

## Publish the Docker image to Docker Hub
```shell
docker login --username chriscarini

VERSION=0.0.1
IMAGE="chriscarini/crypto-to-influxdb"

# Give the image two tags; one version, and one `latest`.
docker build -t "$IMAGE:latest" -t "$IMAGE:$VERSION" .

sudo docker push "$IMAGE:latest" && docker push "$IMAGE:$VERSION"
```