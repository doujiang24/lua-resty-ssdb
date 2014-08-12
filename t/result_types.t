# vim:set ft= ts=4 sw=4 et:

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(2);

plan tests => repeat_each() * (3 * blocks());

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
};

$ENV{TEST_NGINX_RESOLVER} = '8.8.8.8';
$ENV{TEST_NGINX_SSDB_PORT} ||= 8888;

no_long_string();
#no_diff();

run_tests();

__DATA__

=== TEST 1: one number
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local ssdb = require "resty.ssdb"
            local db = ssdb:new()

            db:set_timeout(1000) -- 1 sec

            local ok, err = db:connect("127.0.0.1", $TEST_NGINX_SSDB_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            res, err = db:del("number")
            if not res then
                ngx.say("failed to set number: ", err)
                return
            end

            ngx.say("del number: ", res)

            for i = 1, 2 do
                res, err = db:setnx("number", 10)
                if not res then
                    ngx.say("failed to set number: ", err)
                    return
                end

                ngx.say("setnx number: ", res)
            end

            db:close()
        ';
    }
--- request
GET /t
--- response_body
del number: 1
setnx number: 1
setnx number: 0
--- no_error_log
[error]



=== TEST 2: one string
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local ssdb = require "resty.ssdb"
            local db = ssdb:new()

            db:set_timeout(1000) -- 1 sec

            local ok, err = db:connect("127.0.0.1", $TEST_NGINX_SSDB_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            res, err = db:set("number", "10")
            if not res then
                ngx.say("failed to set number: ", err)
                return
            end

            ngx.say("set number: ", res)

            res, err = db:incr("number")
            if not res then
                ngx.say("failed to incr number: ", err)
                return
            end

            local res, err = db:get("number")
            if err then
                ngx.say("failed to get number: ", err)
                return
            end

            if not res then
                ngx.say("dog not found.")
                return
            end

            ngx.say("number: ", res, ", ", type(res))

            db:close()
        ';
    }
--- request
GET /t
--- response_body
set number: 1
number: 11, string
--- no_error_log
[error]



=== TEST 3: array string
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local ssdb = require "resty.ssdb"
            local db = ssdb:new()

            db:set_timeout(1000) -- 1 sec

            local ok, err = db:connect("127.0.0.1", $TEST_NGINX_SSDB_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            res, err = db:hclear("hashkey")
            if err then
                ngx.say("failed to hclear hashkey: ", err)
                return
            end

            for i = 1, 3 do
                res, err = db:hset("hashkey", i, 1)
                if err then
                    ngx.say("failed to hset hashkey: ", err)
                    return
                end
            end

            res, err = db:hkeys("hashkey", "", "", 10)
            if err then
                ngx.say("failed to get: ", err)
                return
            end

            if res == ngx.null then
                ngx.say("hashkey not found.")
                return
            end

            local cjson = require "cjson"
            ngx.say("hkeys result: ", cjson.encode(res))

            db:close()
        ';
    }
--- request
GET /t
--- response_body
hkeys result: ["1","2","3"]
--- no_error_log
[error]



=== TEST 4: array string may turn to hash
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua '
            local ssdb = require "resty.ssdb"
            local db = ssdb:new()

            db:set_timeout(1000) -- 1 sec

            local ok, err = db:connect("127.0.0.1", $TEST_NGINX_SSDB_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            res, err = db:hclear("hashkey")
            if err then
                ngx.say("failed to hclear hashkey: ", err)
                return
            end

            for i = 1, 3 do
                res, err = db:hset("hashkey", i, 1)
                if err then
                    ngx.say("failed to hset hashkey: ", err)
                    return
                end
            end

            res, err = db:hscan("hashkey", "", "", 10)
            if err then
                ngx.say("failed to get: ", err)
                return
            end

            if res == ngx.null then
                ngx.say("hashkey not found.")
                return
            end

            local cjson = require "cjson"
            ngx.say("hscan result: ", cjson.encode(res))

            db:close()
        ';
    }
--- request
GET /t
--- response_body
hscan result: ["1","1","2","1","3","1"]
--- no_error_log
[error]
