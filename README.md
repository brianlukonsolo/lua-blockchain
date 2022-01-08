# lua-blockchain by Brian Lukonsolo
 A personal Lua project where I attempt to create a basic blockchain running using Lua code within a Docker openresty/nginx container environment.
 
### Installation instructions:
 
 Install Docker
 - clone this repository and navigate into the cloned directory root
 - run the command 'docker build . --tag lua-blockchain'
 - run the command 'docker run -it -p 8080 lua-blockchain sh'
 - Once the container starts a shell, cd into the 'lua-blockchain' directory
 - run the command 'lapis server'
 - check that the server is running by navigating to http://127.0.0.1:8080/ on your local machine
 
### Technologies used:
 
 - Lua
 - Docker
 - Maven
 - Openresty

### Project dependencies:

 - Lua
 - luarocks
 - libssl-dev
 - openssl
 - lapis
    - ansicolors
    - date
    - etlua
    - loadkit &&\
    - lpeg &&\
    - lua-cjson &&\
    - luafilesystem &&\
    - luasocket &&\
    - pgmoon &&\
    - luaossl CRYPTO_DIR=/usr/ ##OPENSSL_LIBDIR=OPENSSL_DIR=/usr/include/openssl/


