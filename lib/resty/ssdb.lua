-- Copyright (C) 2013 LazyZhu (lazyzhu.com)
-- Copyright (C) 2013 IdeaWu (ideawu.com)
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
local remove = table.remove


local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end


local _M = new_tab(0, 56)
_M._VERSION = '0.20'


local commands = {
    "set",                  "get",                 "del",
    "scan",                 "rscan",               "keys",
    "incr",                 "decr",                "exists",
    "multi_set",            "multi_get",           "multi_del",
    "multi_exists",
    "hset",                 "hget",                "hdel",
    "hscan",                "hrscan",              "hkeys",
    "hincr",                "hdecr",               "hexists",
    "hsize",                "hlist",
    --[[ "multi_hset", ]]   "multi_hget",          "multi_hdel",
    "multi_hexists",        "multi_hsize",
    "zset",                 "zget",                "zdel",
    "zscan",                "zrscan",              "zkeys",
    "zincr",                "zdecr",               "zexists",
    "zsize",                "zlist",
    --[[ "multi_zset", ]]   "multi_zget",          "multi_zdel",
    "multi_zexists",        "multi_zsize",
}



local mt = { __index = _M }


function _M.new(self)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end
    return setmetatable({ sock = sock }, mt)
end


function _M.set_timeout(self, timeout)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(timeout)
end


function _M.connect(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:connect(...)
end


function _M.set_keepalive(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:setkeepalive(...)
end


function _M.get_reused_times(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:getreusedtimes()
end


function _M.close(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:close()
end


local function _read_reply(sock)
	local val = {}

	while true do
		-- read block size
		local line, err, partial = sock:receive()
		if not line or len(line)==0 then
			-- packet end
			break
		end
		local d_len = tonumber(line)

		-- read block data
		local data, err, partial = sock:receive(d_len)
		insert(val, data);

		-- ignore the trailing lf/crlf after block data
		local line, err, partial = sock:receive()
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
            return nil
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


function _M.multi_hset(self, hashname, ...)
    local args = {...}
    if #args == 1 then
        local t = args[1]
        local array = {}
        for k, v in pairs(t) do
            insert(array, k)
            insert(array, v)
        end
        -- print("key", hashname)
        return _do_cmd(self, "multi_hset", hashname, unpack(array))
    end

    -- backwards compatibility
    return _do_cmd(self, "multi_hset", hashname, ...)
end


function _M.multi_zset(self, keyname, ...)
    local args = {...}
    if #args == 1 then
        local t = args[1]
        local array = {}
        for k, v in pairs(t) do
            insert(array, k)
            insert(array, v)
        end
        -- print("key", keyname)
        return _do_cmd(self, "multi_zset", keyname, unpack(array))
    end

    -- backwards compatibility
    return _do_cmd(self, "multi_zset", keyname, ...)
end


function _M.init_pipeline(self)
    self._reqs = {}
end


function _M.cancel_pipeline(self)
    self._reqs = nil
end


function _M.commit_pipeline(self)
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


function _M.array_to_hash(self, t)
    local h = {}
    for i = 1, #t, 2 do
        h[t[i]] = t[i + 1]
    end
    return h
end


return _M
