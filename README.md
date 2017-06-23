# lua-nginx-etcd-announcer
A nginx module which allows arbitrary ETCD keys to be set and refreshed at periodic intervals.
The most obvious application would be dynamic registration and discovery of upstream nginx instances in cojunction with other etcd-aware modules such as lua-resty-upstream-etcd or nginx-upsync-module.

Supported features:
* basic authentication
* customizable TTL, timeout and refresh intervals

## Requirements
* [lua-nginx-module](https://github.com/openresty/lua-nginx-module)
* [lua-resty-http](https://github.com/pintsized/lua-resty-http)
* cjson

## Configuration

```lua
server {
        ...

        lua_shared_dict etcd 32k;

        init_worker_by_lua '
             local etcd = require("etcd")
             local client = etcd.client("foo:bar@127.0.0.1:2379")
             local opts -- ttl, refresh, timeout
             client:announce("http/servers/127.0.0.1:80", "{\"weight\":2, \"max_fails\":2, \"fail_timeout\":10, \"down\":1}", ngx.shared.etcd, opts)
        ';

        ...
}
```
