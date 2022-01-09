# lua-blockchain by Brian Lukonsolo
 A personal Lua project where I attempt to create a basic blockchain running using Lua code within a Docker openresty/nginx container environment.
 
### Usage instructions:
 
 #### Local Docker environment
 - clone this repository and navigate into the cloned directory root
 - run the command `docker build . --tag lua-blockchain`
 - run the command `docker run -it -p 8080 lua-blockchain`
 - check that the server is running by navigating to `http://127.0.0.1:8080/` on your local machine
 
### Technologies used:
 
 - Lua
 - Docker
 - Maven
 - Openresty

### Project dependencies:

 - Lua version 5.1.5 (for main code logic)
 - luarocks version 2.4.2
 - libssl-dev
 - openssl version 1.1.1k 25 Mar 2021
 
 - lapis version 1.9.0-1 (for HTTP API endpoint calls)
    - ansicolors    version 1.0.2-3
    - date          version 2.2-2
    - etlua         version 1.3.0-1
    - loadkit       version 1.1.0-1
    - lpeg          version 1.1.0-1
    - lua-cjson     version 1.1.0-1
    - luafilesystem version 1.1.0-1
    - luasocket     version 1.1.0-1
    - pgmoon        version 1.1.0-1
    - luaossl       version 1.1.0-1

 - busted version 2.0.0 (for unit testing)


