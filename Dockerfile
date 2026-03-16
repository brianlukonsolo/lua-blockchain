FROM openresty/openresty:latest

LABEL version="9.0.0" \
      maintainer="Brian M Lukonsolo"

RUN apt-get update && \
    apt-get install -y --no-install-recommends lua-socket lua-sec lua-cjson lua-sql-sqlite3 ca-certificates luajit openssl && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app/data /tests

COPY src/main/lua/com/brianlukonsolo/ /app/
COPY src/test/ /tests/

RUN chmod +x /app/entrypoint.sh

EXPOSE 8080 19100 19090/udp

ENTRYPOINT ["/app/entrypoint.sh"]

CMD ["openresty", "-p", "/app/", "-c", "/app/nginx.conf"]
