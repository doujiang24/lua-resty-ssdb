-- Copyright (C) 2013 LazyZhu (lazyzhu.com)
-- Copyright (C) 2012 Yichun Zhang (agentzh)


local sub = string.sub
local tcp = ngx.socket.tcp
local insert = table.insert
local concat = table.concat
local len = string.len
local null = ngx.null
local pairs = pairs
local unpack = unpack
local setmetatable = setmetatable
local tonumber = tonumber
local error = error
local gmatch = string.gmatch
local remove = table.remove


module(...)

_VERSION = '0.01'

local commands = {
    "set",               "get",               "del",
    "scan",              "rscan",             "keys",
    "incr",              "decr",
    "zset",              "zget",              "zdel",
    "zscan",             "zrscan",            "zkeys",
    "zsize",             "zlist",
    "zincr",             "zdecr",
    "hset",              "hget",              "hdel",
    "hscan",             "hrscan",            "hkeys",
    "hsize",             "hlist",
    "hincr",             "hdecr",
    "multi_set",         "multi_get",         "multi_del"
}


local mt = { __index = _M }


function new(self)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end
    return setmetatable({ sock = sock }, mt)
end


function set_timeout(self, timeout)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end


function connect(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:connect(...)
end


function set_keepalive(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:setkeepalive(...)
end


function get_reused_times(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:getreusedtimes()
end


function close(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:close()
end


local function _read_reply(sock)
    local data_reader = sock:receiveuntil("\n\n", { inclusive = true })
    if not data_reader then
        return nil, err
    end

    local data, err = data_reader()
    if not data then
        return nil, err
    end

    local val = {}

    for v in gmatch(data, "%d+\n([^\n]+)\n") do
        insert(val, v)
    end

    local v_num = tonumber(#val)

    if v_num == 1 then
        return val
    else
        remove(val,1)
        return val
    end
end


local function _gen_req(args)
    local req = {}

    for i = 1, #args do
        local arg = args[i]

        if arg then
            insert(req, len(arg))
            insert(req, "\n")
            insert(req, arg)
            insert(req, "\n")
        else
            return nil, err
        end
    end
    insert(req, "\n")

    -- it is faster to do string concatenation on the Lua land
    -- print("request: ", table.concat(req, ""))

    return concat(req, "")
end


local function _do_cmd(self, ...)
    local args = {...}

    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local req = _gen_req(args)

    local reqs = self._reqs
    if reqs then
        insert(reqs, req)
        return
    end

    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end

    return _read_reply(sock)
end


for i = 1, #commands do
    local cmd = commands[i]

    _M[cmd] =
        function (self, ...)
            return _do_cmd(self, cmd, ...)
        end
end


--[=[
function hmset(self, hashname, ...)
    local args = {...}
    if #args == 1 then
        local t = args[1]
        local array = {}
        for k, v in pairs(t) do
            insert(array, k)
            insert(array, v)
        end
        -- print("key", hashname)
        return _do_cmd(self, "hmset", hashname, unpack(array))
    end

    -- backwards compatibility
    return _do_cmd(self, "hmset", hashname, ...)
end
--]=]


function init_pipeline(self)
    self._reqs = {}
end


function cancel_pipeline(self)
    self._reqs = nil
end


function commit_pipeline(self)
    local reqs = self._reqs
    if not reqs then
        return nil, "no pipeline"
    end

    self._reqs = nil

    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local bytes, err = sock:send(reqs)
    if not bytes then
        return nil, err
    end

    local vals = {}
    for i = 1, #reqs do
        local res, err = _read_reply(sock)
        if res then
            insert(vals, res)

        elseif res == nil then
            return nil, err

        else
            insert(vals, err)
        end
    end

    return vals
end


function array_to_hash(self, t)
    local h = {}
    for i = 1, #t, 2 do
        h[t[i]] = t[i + 1]
    end
    return h
end


local class_mt = {
    -- to prevent use of casual module global variables
    __newindex = function (table, key, val)
        error('attempt to write to undeclared variable "' .. key .. '"')
    end
}


function add_commands(...)
    local cmds = {...}
    local newindex = class_mt.__newindex
    class_mt.__newindex = nil
    for i = 1, #cmds do
        local cmd = cmds[i]
        _M[cmd] =
            function (self, ...)
                return _do_cmd(self, cmd, ...)
            end
    end
    class_mt.__newindex = newindex
end


setmetatable(_M, class_mt)
