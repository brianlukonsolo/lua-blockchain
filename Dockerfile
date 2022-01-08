FROM openresty/openresty

LABEL version="1.0" \
      maintainer="Brian M Lukonsolo"

#setup
COPY src/main/lua/com/brianlukonsolo/ /
ENV LUAROCKS_INSTALL='/usr/bin/luarocks install' \
    APT_GET_INSTALL='apt-get -y install'
EXPOSE 8080/tcp

# install luarocks and openssl
RUN apt-get update &&\
    ${APT_GET_INSTALL} luarocks libssl-dev openssl vim

# install lapis and its dependencies
RUN ${LUAROCKS_INSTALL} ansicolors &&\
    ${LUAROCKS_INSTALL} date &&\
    ${LUAROCKS_INSTALL} etlua &&\
    ${LUAROCKS_INSTALL} loadkit &&\
    ${LUAROCKS_INSTALL} lpeg &&\
    ${LUAROCKS_INSTALL} lua-cjson &&\
    ${LUAROCKS_INSTALL} luafilesystem &&\
    ${LUAROCKS_INSTALL} luasocket &&\
    ${LUAROCKS_INSTALL} pgmoon &&\
    ${LUAROCKS_INSTALL} luaossl CRYPTO_DIR=/usr/ ##OPENSSL_LIBDIR=OPENSSL_DIR=/usr/include/openssl/
RUN ${LUAROCKS_INSTALL} lapis

ENTRYPOINT ["lapis", "server"]