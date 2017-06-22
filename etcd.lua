module("etcd", package.seeall)

local ngx = ngx
local math = math
local http = require("resty.http")
local json = require("cjson")

function debugf(fmt, ...)
    ngx.log(ngx.DEBUG, string.format(fmt, ...))
end

function infof(fmt, ...)
    ngx.log(ngx.INFO, string.format(fmt, ...))
end

function errorf(fmt, ...)
    ngx.log(ngx.ERR, string.format(fmt, ...))
end

function base64_encode(str)
    return ngx.encode_base64(str)
end

function uri_parse(str)
    local m = ngx.re.match(str, "((http|https)://)?([^/?]*)([^#]*)(.*)")
    if m then
        local login, pass
        local host, port = m[3], nil
        local path = m[4]
        local anchor = m[5]
        if path:sub(1, 1) ~= "/" then
            path = "/" .. path
        end

        m = ngx.re.match(host, "^([^@]+)@(.+)$")
        if m then
            login, host = m[1], m[2]
        end

        if login then
            m = ngx.re.match(login, "^([^:]+):(.+)$")
            if m then
                login, pass = m[1], m[2]
            end
        end

        m = ngx.re.match(host, "^([^:]*):([0-9]+)$")
        if m then
            host, port = m[1], m[2]
        end

        return {
            ["uri"] = str,
            ["login"] = login,
            ["password"] = pass,
            ["host"] = host,
            ["port"] = port,
            ["path"] = path,
            ["anchor"] = anchor,
    }
else
    return nil
end
end

function uri_escape(str)
    return ngx.escape_uri(str)
end

function uri_add_param(uri, param, value)
    if string.find(uri, "?", 1, true) then
        return uri.."&"..uri_escape(param).."="..uri_escape(value)
    else
        return uri.."?"..uri_escape(param).."="..uri_escape(value)
    end
end

function uri_join(...)
    local args = {...}

    local url = ""
    for index, a in ipairs(args) do
        if #url == 0 then
            url = a
        else
            if string.sub(a, 1, 1) == "/" then
                if string.sub(url, #url, #url) == "/" then
                    url = url .. string.sub(a, 2)
                else
                    url = url .. a
                end
            else
                if string.sub(url, #url, #url) == "/" then
                    url = url .. a
                else
                    url = url .. "/" .. a
                end
            end
        end
    end
    return url
end

-- REQUEST HANDLE FUNCTIONS

function has_error(response)
    if response.status == 200 or response.status == 201 then
        -- success
        return false
    end
    return true
end

function error_message(response)
    if response.status == 200 or response.status == 201 then
        -- success
        return ""
    end

    if response.status == 595 or response.status == 599 then
        -- http client error
        return response.reason.." (status:"..response.status..")"
    end

    if response.body ~= nil and response.body ~= "" then
        local data = json.decode(response.body)
        if data ~= nil and data.errorCode ~= nil and data.message ~= nil then
            -- etcd response
            return data.message.." (code:"..data.errorCode..")"
        end
    end

    return response.reason.." (status:"..response.status..")"
end


-- <CLIENT>

local Client = {}

function Client.clone(self)
    return client(self.addr, {
        user = self.user,
        password = self.password,
        timeout = self.timeout,
        })
end

function Client.request(self, method, query, timeout)
    local headers = {}

    local u = uri_parse(self.addr)
    if u["login"] ~= nil then
        local pass = u["password"]
        if pass == nil then
            pass = ""
        end
        headers["Authorization"] = "Basic "..base64_encode(u["login"]..":"..pass)
    end

    local url = uri_join("http://", self.addr, "/v2/keys", query)

    if timeout == nil then
        timeout = self.timeout
    end

    debugf("%s %s (timeout=%d)", method, url, timeout)

    local h = http.new()
    h:set_timeout(timeout * 1000)

    local r, err = h:request_uri(url, {method = method, headers = headers})
    
    if r == nil then
        r = {status  = 595, reason  = err}
    end

    if r["headers"] ~= nil and r["headers"]["x-etcd-index"] ~= nil then
        self.index = tonumber(r["headers"]["x-etcd-index"])
        debugf("x-etcd-index: %d", self.index)
    end

    if has_error(r) then
        debugf("%s %s (timeout=%d): %s", method, url, timeout, error_message(r))
    else
        debugf("%s %s (timeout=%d): success", method, url, timeout)
    end

    return r
end

function Client.get(self, key, opts)
    if opts == nil then
        opts = {}
    end

    local url = key

    if opts["recursive"] then
        url = uri_add_param(url, "recursive", "true")
    end
    if opts["sort"] then
        url = uri_add_param(url, "sorted", "true")
    end

    return self:request("GET", url, opts["timeout"])
end

function Client.set(self, key, value, opts)
    if opts == nil then
        opts = {}
    end


    local url = key
    if value ~= nil then
        url = uri_add_param(url, "value", value)
    end

    if opts["ttl"] ~= nil then
        url = uri_add_param(url, "ttl", opts["ttl"])
    end

    return self:request("PUT", url, opts["timeout"])
end

function Client.create(self, key, value, opts)
    return self:set(uri_add_param(key, "prevExist", "false"), value, opts)
end

function Client.update(self, key, value, opts)
    return self:set(uri_add_param(key, "prevExist", "true"), value, opts)
end

function Client.refresh(self, key, opts)
    return self:set(uri_add_param(uri_add_param(key, "prevExist", "true"), "refresh", "true"), nil, opts)
end

function Client.cas(self, key, old_value, new_value, opts)
    return self:set(uri_add_param(uri_add_param(key, "prevValue", old_value), "prevExist", "true"), new_value, opts)
end

function Client.wait(self, key, opts)
    -- TODO: while true waiting. Continue on timeout
    return self:get(uri_add_param(uri_add_param(key, "wait", "true"), "waitIndex", self.index+1), opts)
end

function client(addr, opts)
    if opts == nil then
        opts = {}
    end

    self = {
        addr = addr,
        index = 0,
        user = opts["user"],
        password = opts["password"],
        timeout = opts["timeout"],
    }

    if self.timeout == nil then
        self.timeout = 1
    end

    setmetatable(self, {__index = Client})

    return self
end

-- </CLIENT>

-- <DISCOVERY>
function Client.announce(self, key, value, shm, opts)
    local client = self:clone()

    if opts == nil then
        opts = {}
    end

    if opts.ttl == nil then
        opts.ttl = 60 -- one minute
    end
    if opts.refresh == nil then
        opts.refresh = 10 -- every 10 seconds
    end
    if opts.timeout == nil then
        opts.timeout = 5
    end

    function refresh()
        local schedule, think
        local lock_key = key .. "_lock"

        schedule = function(interval)
            local ok, err = ngx.timer.at(interval, think)
            if not ok then
                etcd.errorf("failed to create timer: %s", err)
            end
        end

        think = function()
            if not shm:add(lock_key, true, opts.refresh) then 
                schedule(math.random(1, opts.refresh))
                return
            end

            local r = client:refresh(key, {timeout=opts.timeout, ttl=opts.ttl})

            local has_error = etcd.has_error(r)
            if has_error then
                etcd.errorf("announce %s refresh error: %s", key, etcd.error_message(r))

                r = client:set(key, value, {timeout=opts.timeout, ttl=opts.ttl})
                has_error = etcd.has_error(r)

                if has_error then
                    etcd.errorf("announce %s set error: %s", key, etcd.error_message(r))
                end
            end

            if has_error then
                shm:set(lock_key, true, 1)
                schedule(1)
            else
                schedule(opts.refresh)
            end
        end

        return schedule(0.1)
    end

    return refresh()
end

-- </DISCOVERY>
