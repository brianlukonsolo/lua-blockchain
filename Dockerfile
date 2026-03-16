FROM openresty/openresty:latest

LABEL version="2.0.0" \
      maintainer="Brian M Lukonsolo"

RUN apt-get update && \
    apt-get install -y --no-install-recommends lua-socket lua-sec lua-cjson ca-certificates luajit && \
    rm -rf /var/lib/apt/lists/*

COPY src/main/lua/com/brianlukonsolo/ /app/
COPY src/test/ /tests/

EXPOSE 8080

CMD ["openresty", "-p", "/app/", "-c", "/app/nginx.conf"]
