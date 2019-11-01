FROM dgraph/dgraph:latest
LABEL MAINTAINER="Dgraph Labs <contact@dgraph.io>"

# Ratel port
EXPOSE 8000
# REST API port
EXPOSE 8080
# gRPC API port
EXPOSE 9080
# graphql port
EXPOSE 9000

COPY dgraph /usr/local/bin/dgraph
ADD run.sh /tmp
RUN chmod +x /tmp/run.sh
CMD ["/tmp/run.sh"]
