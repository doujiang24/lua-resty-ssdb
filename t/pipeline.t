# vim:set ft= ts=4 sw=4 et:

use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(2);

plan tests => repeat_each() * (3 * blocks());

my $pwd = cwd();

our $HttpConfig = qq{
    lua_package_path "$pwd/lib/?.lua;;;";
    lua_package_cpath "/usr/local/openresty-debug/lualib/?.so;/usr/local/openresty/lualib/?.so;;";
};

$ENV{TEST_NGINX_RESOLVER} = '8.8.8.8';
$ENV{TEST_NGINX_SSDB_PORT} ||= 8888;

no_long_string();
#no_diff();

run_tests();

__DATA__

=== TEST 1: basic
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
                db:init_pipeline()

                db:set("dog", "an animal")
                db:get("dog")
                db:set("dog", "hello")
                db:get("dog")

                local results = db:commit_pipeline()
                local cjson = require "cjson"
                ngx.say(cjson.encode(results))
            end

            db:close()
        ';
    }
--- request
GET /t
--- response_body
[1,"an animal",1,"hello"]
[1,"an animal",1,"hello"]
--- no_error_log
[error]

