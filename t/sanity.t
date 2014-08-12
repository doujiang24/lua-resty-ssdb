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

=== TEST 1: set and get
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

            res, err = db:set("dog", "an animal")
            if not res then
                ngx.say("failed to set dog: ", err)
                return
            end

            ngx.say("set dog: ", res)

            for i = 1, 2 do
                local res, err = db:get("dog")
                if err then
                    ngx.say("failed to get dog: ", err)
                    return
                end

                if not res then
                    ngx.say("dog not found.")
                    return
                end

                ngx.say("dog: ", res)
            end

            db:close()
        ';
    }
--- request
GET /t
--- response_body
set dog: 1
dog: an animal
dog: an animal
--- no_error_log
[error]



=== TEST 2: get nil bulk value
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

            for i = 1, 2 do
                res, err = db:get("not_found")
                if err then
                    ngx.say("failed to get: ", err)
                    return
                end

                if res == ngx.null then
                    ngx.say("not_found not found.")
                    return
                end

                ngx.say("get not_found: ", res)
            end

            db:close()
        ';
    }
--- request
GET /t
--- response_body
not_found not found.
--- no_error_log
[error]



=== TEST 3: get nil list
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

            for i = 1, 2 do
                res, err = db:hkeys("nokey", "", "", 10)
                if err then
                    ngx.say("failed to get: ", err)
                    return
                end

                if res == ngx.null then
                    ngx.say("nokey not found.")
                    return
                end

                ngx.say("get nokey: ", #res, " (", type(res), ")")
            end

            db:close()
        ';
    }
--- request
GET /t
--- response_body
get nokey: 0 (table)
get nokey: 0 (table)
--- no_error_log
[error]



=== TEST 4: incr and decr
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

            res, err = db:set("connections", 10)
            if not res then
                ngx.say("failed to set connections: ", err)
                return
            end

            ngx.say("set connections: ", res)

            res, err = db:incr("connections")
            if not res then
                ngx.say("failed to incr connections: ", err)
                return
            end

            ngx.say("incr connections: ", res)

            local res, err = db:get("connections")
            if err then
                ngx.say("failed to get connections: ", err)
                return
            end

            res, err = db:incr("connections")
            if not res then
                ngx.say("failed to incr connections: ", err)
                return
            end

            ngx.say("incr connections: ", res)

            res, err = db:decr("connections")
            if not res then
                ngx.say("failed to decr connections: ", err)
                return
            end

            ngx.say("decr connections: ", res)

            res, err = db:get("connections")
            if not res then
                ngx.say("connections not found.")
                return
            end

            ngx.say("connections: ", res)

            res, err = db:del("connections")
            if not res then
                ngx.say("failed to del connections: ", err)
                return
            end

            ngx.say("del connections: ", res)

            res, err = db:incr("connections")
            if not res then
                ngx.say("failed to set connections: ", err)
                return
            end

            ngx.say("incr connections: ", res)

            res, err = db:get("connections")
            if not res then
                ngx.say("connections not found.")
                return
            end

            ngx.say("connections: ", res)

            db:close()
        ';
    }
--- request
GET /t
--- response_body
set connections: 1
incr connections: 11
incr connections: 12
decr connections: 11
connections: 11
del connections: 1
incr connections: 1
connections: 1
--- no_error_log
[error]



=== TEST 5: incr by step
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

            res, err = db:set("connections", 10)
            if not res then
                ngx.say("failed to set connections: ", err)
                return
            end

            ngx.say("set connections: ", res)

            res, err = db:incr("connections", 12)
            if not res then
                ngx.say("failed to set connections: ", res, ": ", err)
                return
            end

            ngx.say("incr connections: ", res)

            db:close()
        ';
    }
--- request
GET /t
--- response_body
set connections: 1
incr connections: 22
--- no_error_log
[error]



=== TEST 6: qpush and qslice
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

            local res, err = db:qclear("mylist")
            if not res then
                ngx.say("failed to qclear: ", err)
                return
            end

            local res, err = db:qpush("mylist", "hello")
            if not res then
                ngx.say("failed to qpush: ", err)
                return
            end
            ngx.say("qpush result: ", res)

            res, err = db:qpush("mylist", "world")
            if not res then
                ngx.say("failed to qpush: ", err)
                return
            end
            ngx.say("qpush result: ", res)

            res, err = db:qslice("mylist", 0, -1)
            if not res then
                ngx.say("failed to qslice: ", err)
                return
            end
            local cjson = require "cjson"
            ngx.say("qslice result: ", cjson.encode(res))

            db:close()
        ';
    }
--- request
GET /t
--- response_body
qpush result: 1
qpush result: 2
qslice result: ["hello","world"]
--- no_error_log
[error]



=== TEST 7: set keepalive and get reused times
--- http_config eval: $::HttpConfig
--- config
    resolver $TEST_NGINX_RESOLVER;
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

            local times = db:get_reused_times()
            ngx.say("reused times: ", times)

            local ok, err = db:set_keepalive()
            if not ok then
                ngx.say("failed to set keepalive: ", err)
                return
            end

            ok, err = db:connect("127.0.0.1", $TEST_NGINX_SSDB_PORT)
            if not ok then
                ngx.say("failed to connect: ", err)
                return
            end

            times = db:get_reused_times()
            ngx.say("reused times: ", times)
        ';
    }
--- request
GET /t
--- response_body
reused times: 0
reused times: 1
--- no_error_log
[error]



=== TEST 8: multi_get
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

            res, err = db:set("dog", "an animal")
            if not res then
                ngx.say("failed to set dog: ", err)
                return
            end

            ngx.say("set dog: ", res)

            res, err = db:del("cat")
            if not res then
                ngx.say("failed to del cat: ", err)
                return
            end

            for i = 1, 2 do
                local res, err = db:multi_get("dog", "cat", "dog")
                if err then
                    ngx.say("failed to get dog: ", err)
                    return
                end

                if not res then
                    ngx.say("dog not found.")
                    return
                end

                local cjson = require "cjson"
                ngx.say("res: ", cjson.encode(db:array_to_hash(res)))
            end

            db:close()
        ';
    }
--- request
GET /t
--- response_body
set dog: 1
res: {"dog":"an animal"}
res: {"dog":"an animal"}
--- no_error_log
[error]
