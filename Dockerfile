FROM openresty/openresty

LABEL version="1.0" \
      maintainer="Brian M Lukonsolo"

#setup
COPY src/main/lua/com/brianlukonsolo/ /
COPY src/test/ /lua-blockchain-tests/
ENV LUAROCKS_INSTALL='/usr/bin/luarocks install' \
    APT_GET_INSTALL='apt-get -y install'
EXPOSE 8080/tcp

# install luarocks and openssl
RUN apt-get update &&\
    ${APT_GET_INSTALL} luarocks libssl-dev openssl vim

# install lapis and its dependencies
RUN ${LUAROCKS_INSTALL} ansicolors 1.0.2-3 &&\
    ${LUAROCKS_INSTALL} date 2.2-2 &&\
    ${LUAROCKS_INSTALL} etlua 1.3.0-1 &&\
    ${LUAROCKS_INSTALL} loadkit 1.1.0-1 &&\
    ${LUAROCKS_INSTALL} lpeg 1.0.2-1 &&\
    ${LUAROCKS_INSTALL} lua-cjson 2.1.0.6-1 &&\
    ${LUAROCKS_INSTALL} luafilesystem 1.8.0-1 &&\
    ${LUAROCKS_INSTALL} luasocket 3.0rc1-2 &&\
    ${LUAROCKS_INSTALL} pgmoon 1.13.0-1 &&\
    ${LUAROCKS_INSTALL} luaossl 20200709-0 CRYPTO_DIR=/usr/ ##OPENSSL_LIBDIR=OPENSSL_DIR=/usr/include/openssl/
RUN ${LUAROCKS_INSTALL} lapis 1.9.0-1
# install busted unit testing framework
RUN ${LUAROCKS_INSTALL} busted 2.0.0

ENTRYPOINT ["lapis", "server"]