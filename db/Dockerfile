# pull official base image
FROM postgres:17.4

# run create.sql on init
ADD create.sql /docker-entrypoint-initdb.d
