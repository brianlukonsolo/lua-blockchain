FROM openresty/openresty

#setup
ENV APPLICATION_NAME='lua-blockchain' \
    LUAROCKS_INSTALL='/usr/bin/luarocks install' \
    APT_GET_INSTALL='apt-get -y install'
WORKDIR /${APPLICATION_NAME}
EXPOSE 8080/tcp

# install luarocks and openssl
RUN apt-get update &&\
    ${APT_GET_INSTALL} luarocks libssl-dev openssl

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

COPY src/main/lua/com/brianlukonsolo/ lua-blockchain/

# ENTRYPOINT ["lapis", "serve"]