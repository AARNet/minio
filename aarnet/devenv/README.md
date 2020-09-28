# Minio EOS Gateway Development environment

## Synopsis

This docker-compose and script will spin up a local environment EOS environment with minio so that you can test your build locally.

## Usage

If you don't have a copy of the eos-docker images already, run:
```
./devenv -b
```

This will clone the eos-docker repository and build the docker images for starting an EOS instance. Once that's done, just run:
```
./devenv -a
```

This will start the EOS instance and a minio container. With the standard `docker-compose.yml`, minio will be available at `http://localhost:9000` with the credentials both set to `miniodev`. i

So pick a client, and start playing.
