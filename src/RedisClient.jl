# redis client in julia
# wraps the stable hiredis C library

module RedisClient

using Logging

Logging.configure(level=DEBUG)

const REDIS_REPLY_STRING = 1
const REDIS_REPLY_ARRAY = 2
const REDIS_REPLY_INTEGER = 3
const REDIS_REPLY_NIL = 4
const REDIS_REPLY_STATUS = 5
const REDIS_REPLY_ERROR = 6

redisContext = 0

type RedisReadTask
    rtype::Int32
    elements::Int32
    idx::Int32
    obj::Ptr{Void}
    parent::Ptr{RedisReadTask}
    privdata::Ptr{Void}
end

function create_string(task::Ptr{RedisReadTask}, str::Ptr{Uint8}, len::Uint)
    # not implemented
    ret::Ptr{Void} = 0
    ret
end

function create_array(task::Ptr{RedisReadTask}, len::Int32)
    # not implemented
    ret::Ptr{Void} = 0
    ret
end

function create_integer(task::Ptr{RedisReadTask}, int::Int)
    # not implemented
    ret::Ptr{Void} = 0
    ret
end

function create_nil(task::Ptr{RedisReadTask})
    # not implemented
    ret::Ptr{Void} = 0
    ret
end

function free_object(obj::Ptr{Void})
    # not implemented
    ret::Void = 0
    ret
end

const create_string_c = cfunction(create_string, Ptr{Void}, (Ptr{RedisReadTask}, Ptr{Uint8}, Uint))

const create_array_c = cfunction(create_array, Ptr{Void}, (Ptr{RedisReadTask}, Int32))

const create_integer_c = cfunction(create_integer, Ptr{Void}, (Ptr{RedisReadTask}, Int))

const create_nil_c = cfunction(create_nil, Ptr{Void}, (Ptr{RedisReadTask},))

const free_object_c = cfunction(free_object, Void, (Ptr{Void},))

type RedisReplyObjectFunctions
    create_string_c
    create_array_c
    create_integer_c
    create_nil_c
    free_object_c
end

type RedisReader
    err::Int32
    errstr::Ptr{Uint8}
    buf::Ptr{Uint8}
    pos::Uint
    len::Uint
    maxbuf::Uint
    rstack::Array{RedisReadTask}
    ridx::Int32
    reply::Ptr{Void}
    fn::Ptr{RedisReplyObjectFunctions}
    privdata::Ptr{Void}
end

type RedisContext
    err::Int32
    errstr::Ptr{Uint8}
    fd::Int32
    flags::Int32
    obuf::Ptr{Uint8}
    reader::Ptr{RedisReader}
end

type RedisReply
    rtype::Int32                  # REDIS_REPLY_*
    integer::Uint64               # The integer when type is REDIS_REPLY_INTEGER
    len::Int32                    # Length of string
    str::Ptr{Uint8}               # Used for both REDIS_REPLY_ERROR and REDIS_REPLY_STRING
    elements::Uint                # number of elements, for REDIS_REPLY_ARRAY
    element::Ptr{Ptr{RedisReply}} # elements vector for REDIS_REPLY_ARRAY
end

function start_session(host::String = "127.0.0.1", port::Int64 = 6379)
    global redisContext = ccall((:redisConnect, "libhiredis"), Ptr{Uint8}, (Ptr{Uint8}, Int32), host, port)
end

function do_command(command::String)
    if redisContext == 0
        error("redisContext not defined. Please call RedisClient.start_session.")
    end
    debug(string("RedisClient.do_command: ", command))
    redisReply = ccall((:redisCommand, "libhiredis"), Ptr{RedisReply}, (Ptr{Uint8}, Ptr{Uint8}), redisContext, command)
    r = unsafe_load(redisReply)
    if r.rtype == REDIS_REPLY_ERROR
        error(bytestring(r.str))
    end
    ret = nothing
    if r.rtype == REDIS_REPLY_STRING
        ret = bytestring(r.str)
    elseif r.rtype == REDIS_REPLY_INTEGER
        ret = int(r.integer)
    elseif r.rtype == REDIS_REPLY_ARRAY
        results = []
        n = int(r.elements)
        replies = pointer_to_array(r.element, n)
        for i in 1:n
            ri = unsafe_load(replies[i])
            push!(results, bytestring(ri.str))
        end
        ret = results
    end
    ccall((:freeReplyObject, "libhiredis"), Void, (Ptr{RedisReply},), redisReply)
    ret
end

function do_command(argv::Array)
    redisReply = ccall((:redisCommandArgv, "libhiredis"), Ptr{RedisReply}, (Ptr{Uint8}, Int32, Ptr{Ptr{Uint8}}, Ptr{Uint}), redisContext, length(argv), argv, C_NULL)
    r = unsafe_load(redisReply)
    if r.rtype == REDIS_REPLY_ERROR
        error(bytestring(r.str))
    end
    ret = nothing
    if r.rtype == REDIS_REPLY_STRING
        ret = bytestring(r.str)
    elseif r.rtype == REDIS_REPLY_INTEGER
        ret = int(r.integer)
    elseif r.rtype == REDIS_REPLY_ARRAY
        results = []
        n = int(r.elements)
        replies = pointer_to_array(r.element, n)
        for i in 1:n
            ri = unsafe_load(replies[i])
            push!(results, bytestring(ri.str))
        end
        ret = results
    end
    ccall((:freeReplyObject, "libhiredis"), Void, (Ptr{RedisReply},), redisReply)
    ret
end

function kvset(key::String, value::String)
    do_command(string("SET ", key, " ", value))
end

function kvget(key::String)
    do_command(string("GET ", key))
end

function incr(key::String)
    do_command(string("INCR ", key))
end

function hset(key::String, attr_name::String, attr_value)
    #TODO do_command(["HSET %s %s %s", key, attr_name, string(attr_value)])
    do_command(string("HSET ", key, " ", attr_name, " ", attr_value))
end

function hget(key::String, attr_name::String)
    do_command(string("HGET ", key, " ", attr_name))
end

function hmset(key::String, argv...)
    cmd = string("HMSET ", key)
    for arg in argv
        cmd = string(cmd, " ", arg)
    end
    do_command(cmd)
end

function hmset(key::String, attrs::Array)
    cmd = string("HMSET ", key)
    for attr in attrs
        cmd = string(cmd, " ", attr)
    end
    do_command(cmd)
end

function hmget(key::String, argv...)
    cmd = string("HMGET ", key)
    for arg in argv
        cmd = string(cmd, " ", arg)
    end
    do_command(cmd)
end

function hmget(key::String, fields::Array)
    cmd = string("HMGET ", key)
    for field in fields
        cmd = string(cmd, " ", field)
    end
    do_command(cmd)
end

function hgetall(key::String)
    do_command(string("HGETALL ", key))
end

function sadd(key::String, member)
    do_command(string("SADD ", key, " ", string(member)))
end

function smembers(key::String)
    do_command(string("SMEMBERS ", key))
end

export start_session, kvset, kvget, incr, hset, hget, hmset, hmget, hgetall, sadd, smembers

end

# start_session("127.0.0.1", 6379)
# incr("last_customer_key")
# set("foo", "bar")
# get("foo")
# hset("customer:1", "first_name", "Mark")
# hget("customer:1", "first_name")
# hmset("customer:1", "first_name", "Alice", "last_name", "Bean")
# hmget("customer:1", "first_name", "last_name")
# hgetall("customer:1")
# sadd("account_keyset:1", 1)
# sadd("account_keyset:1", 2)
# sadd("account_keyset:1", 3)
# smembers("account_keyset:1")
