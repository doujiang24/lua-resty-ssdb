-- Copyright (C) 2013 LazyZhu (lazyzhu.com)
-- Copyright (C) 2013 IdeaWu (ideawu.com)
-- Copyright (C) 2012 Yichun Zhang (agentzh)


local sub = string.sub
local tcp = ngx.socket.tcp
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


local T_NUMBER = 1
local T_STRING = 2
local T_ARRAY = 3


local _M = new_tab(0, 88)
_M._VERSION = '0.20'

local result_types = {}


local type_commands = {
    {   -- one number
        'getbit',           'setbit',           'countbit',
        'strlen',           'set',              'setx',
        'setnx',            'zset',             'hset',
        'qpush',            'qpush_front',      'qpush_back',
        'del',              'zdel',             'hdel',
        'hsize',            'zsize',            'qsize',
        'hclear',           'zclear',           'qclear',
        'multi_set',        'multi_del',        'multi_hset',
        'multi_hdel',       'multi_zset',       'multi_zdel',
        'incr',             'decr',             'zincr',
        'zdecr',            'hincr',            'hdecr',
        'zget',             'zrank',            'zrrank',
        'zcount',           'zsum',             'zremrangebyrank',
        'zremrangebyscore', 'zavg',
        -- value 1 means exists and 0 not
        'exists',           'hexists',          'zexists',
    },
    {   -- one string
        'get',              'substr',           'getset',
        'hget',             'qget',             'qfront',
        'qback',            'qpop',             'qpop_front',
        'qpop_back',
    },
    {   -- array string
        'keys',             'zkeys',            'hkeys',
        'hlist',            'zlist',            'qslice',
        -- hash string
        'scan',             'rscan',            'hscan',
        'hrscan',           'hgetall',          'multi_hsize',
        'multi_zsize',      'multi_get',        'multi_hget',
        'multi_zget',
        -- hash number
        'zscan',            'zrscan',           'zrange',
        'zrrange',
        -- value 1 means exists and 0 not
        'multi_exists',     'multi_hexists',    'multi_zexists',
    },
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


local function _read_reply(sock, cmd)
    local resp = {}
    local i = 0

	while true do
		-- read block size
		local line, err = sock:receive()
        if not line then
            if err == "timeout" then
                sock:close()
            end
            return nil, err

        elseif #line == 0 then
			-- packet end
			break
		end

		local size = tonumber(line)

		-- read block data
		local data, err = sock:receive(size)
        if not data then
            if err == "timeout" then
                sock:close()
            end
            return nil, err
        end

		-- ignore the trailing lf/crlf after block data
        local dummy, err = sock:receive()
        if not dummy then
            return nil, err
        end

        i = i + 1
        resp[i] = data
	end

    if resp[1] == "ok" then
        local res_typ = result_types[cmd]

        if res_typ == T_NUMBER then
            return tonumber(resp[2])

        elseif res_typ == T_STRING then
            return resp[2]

        elseif res_typ == T_ARRAY then
            remove(resp, 1)
            return resp
        end

        return nil, "invalid command"

    elseif resp[1] == "not_found" then
        return null
    end

    return nil, resp[1]
end


local function _gen_req(args)
    local nargs = #args
    local req = new_tab(nargs + 1, 0)
    local nbits = 1

    for i = 1, #args do
        local arg = args[i]

        if type(arg) ~= "string" then
            arg = tostring(arg)
        end
        req[nbits] = #arg .. "\n" .. arg .. "\n"

        nbits = nbits + 1
    end
    req[nbits] = "\n"

    -- it is faster to do string concatenation on the Lua land
    return concat(req)
end


local function _do_cmd(self, cmd, ...)
    local args = { cmd, ...}

    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local cmds = self._cmds
    if cmds then
        cmds[#cmds + 1] = args
        return
    end

    local req = _gen_req(args)

    local bytes, err = sock:send(req)
    if not bytes then
        return nil, err
    end

    return _read_reply(sock, cmd)
end


for res_typ = 1, #type_commands do
    local commands = type_commands[res_typ]

    for i = 1, #commands do
        local cmd = commands[i]

        _M[cmd] =
            function (self, ...)
                return _do_cmd(self, cmd, ...)
            end

        result_types[cmd] = res_typ
    end
end


function _M.multi_hset(self, hashname, ...)
    local args = {...}
    if #args == 1 then
        local t = args[1]

        local n = 0
        for k, v in pairs(t) do
            n = n + 2
        end

        local array = new_tab(n, 0)

        local i = 0
        for k, v in pairs(t) do
            array[i + 1] = k
            array[i + 2] = v
            i = i + 2
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

        local n = 0
        for k, v in pairs(t) do
            n = n + 2
        end

        local array = new_tab(n, 0)

        local i = 0
        for k, v in pairs(t) do
            array[i + 1] = k
            array[i + 2] = v
            i = i + 2
        end
        -- print("key", keyname)
        return _do_cmd(self, "multi_zset", keyname, unpack(array))
    end

    -- backwards compatibility
    return _do_cmd(self, "multi_zset", keyname, ...)
end


function _M.init_pipeline(self)
    self._cmds = {}
end


function _M.cancel_pipeline(self)
    self._cmds = nil
end


function _M.commit_pipeline(self)
    local cmds = self._cmds
    if not cmds then
        return nil, "no pipeline"
    end

    self._cmds = nil

    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    local ncmds = #cmds
    local reqs = new_tab(ncmds, 0)
    for i = 1, ncmds do
        reqs[i] = _gen_req(cmds[i])
    end

    local bytes, err = sock:send(reqs)
    if not bytes then
        return nil, err
    end

    local vals = new_tab(ncmds, 0)

    for i = 1, ncmds do
        local res, err = _read_reply(sock, cmds[i][1])
        if res then
            vals[i] = res

        elseif res == nil then
            if err == "timeout" then
                close(self)
            end
            return nil, err

        else
            -- be a valid ssdb error value
            vals[i] = { false, err }
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
