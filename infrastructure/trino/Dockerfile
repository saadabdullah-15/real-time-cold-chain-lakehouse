ARG TRINO_VERSION=483
FROM trinodb/trino:${TRINO_VERSION}

USER root
COPY infrastructure/trino/entrypoint.sh /usr/local/bin/coldchain-trino
RUN chmod 0755 /usr/local/bin/coldchain-trino \
    && mkdir -p /etc/trino/catalog \
    && chown -R trino:trino /etc/trino/catalog
USER trino
ENTRYPOINT ["/usr/local/bin/coldchain-trino"]
