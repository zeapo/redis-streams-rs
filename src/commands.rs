use crate::types::{StreamClaimOptions, StreamMaxlen, StreamReadOptions};

use redis::{cmd, ConnectionLike, FromRedisValue, RedisResult, ToRedisArgs};

pub trait StreamCommands: ConnectionLike + Sized {
    #[inline]
    fn ping<RV: FromRedisValue>(&mut self) -> RedisResult<RV> {
        cmd("PING").query(self)
    }

    /// stream commands
    /// XACK <key> <group> <id> <id> ... <id>
    #[inline]
    fn xack<K: ToRedisArgs, G: ToRedisArgs, ID: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        group: G,
        ids: &[ID],
    ) -> RedisResult<RV> {
        cmd("XACK").arg(key).arg(group).arg(ids).query(self)
    }

    /// XADD key <ID or *> [field value] [field value] ...
    #[inline]
    fn xadd<K: ToRedisArgs, ID: ToRedisArgs, F: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        id: ID,
        items: &[(F, V)],
    ) -> RedisResult<RV> {
        cmd("XADD").arg(key).arg(id).arg(items).query(self)
    }

    /// XADD key <ID or *> [rust BTreeMap] ...
    #[inline]
    fn xadd_map<K: ToRedisArgs, ID: ToRedisArgs, BTM: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        id: ID,
        map: BTM,
    ) -> RedisResult<RV> {
        cmd("XADD").arg(key).arg(id).arg(map).query(self)
    }

    /// XADD key [MAXLEN [~|=] <count>] <ID or *> [field value] [field value] ...
    #[inline]
    fn xadd_maxlen<
        K: ToRedisArgs,
        ID: ToRedisArgs,
        F: ToRedisArgs,
        V: ToRedisArgs,
        RV: FromRedisValue,
    >(
        &mut self,
        key: K,
        maxlen: StreamMaxlen,
        id: ID,
        items: &[(F, V)],
    ) -> RedisResult<RV> {
        cmd("XADD")
            .arg(key)
            .arg(maxlen)
            .arg(id)
            .arg(items)
            .query(self)
    }

    /// XCLAIM <key> <group> <consumer> <min-idle-time> <ID-1> <ID-2>
    #[inline]
    fn xclaim<
        K: ToRedisArgs,
        G: ToRedisArgs,
        C: ToRedisArgs,
        MIT: ToRedisArgs,
        ID: ToRedisArgs,
        RV: FromRedisValue,
    >(
        &mut self,
        key: K,
        group: G,
        consumer: C,
        min_idle_time: MIT,
        ids: &[ID],
    ) -> RedisResult<RV> {
        cmd("XCLAIM")
            .arg(key)
            .arg(group)
            .arg(consumer)
            .arg(min_idle_time)
            .arg(ids)
            .query(self)
    }

    /// XCLAIM <key> <group> <consumer> <min-idle-time> <ID-1> <ID-2>
    ///     [IDLE <milliseconds>] [TIME <mstime>] [RETRYCOUNT <count>]
    ///     [FORCE] [JUSTID]
    #[inline]
    fn xclaim_options<
        K: ToRedisArgs,
        G: ToRedisArgs,
        C: ToRedisArgs,
        MIT: ToRedisArgs,
        ID: ToRedisArgs,
        RV: FromRedisValue,
    >(
        &mut self,
        key: K,
        group: G,
        consumer: C,
        min_idle_time: MIT,
        ids: &[ID],
        options: StreamClaimOptions,
    ) -> RedisResult<RV> {
        cmd("XCLAIM")
            .arg(key)
            .arg(group)
            .arg(consumer)
            .arg(min_idle_time)
            .arg(ids)
            .arg(options)
            .query(self)
    }

    /// XDEL <key> [<ID1> <ID2> ... <IDN>]
    ///
    #[inline]
    fn xdel<K: ToRedisArgs, ID: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        ids: &[ID],
    ) -> RedisResult<RV> {
        cmd("XDEL").arg(key).arg(ids).query(self)
    }

    /// XGROUP CREATE <key> <groupname> <id or $>
    #[inline]
    fn xgroup_create<K: ToRedisArgs, G: ToRedisArgs, ID: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        group: G,
        id: ID,
    ) -> RedisResult<RV> {
        cmd("XGROUP")
            .arg("CREATE")
            .arg(key)
            .arg(group)
            .arg(id)
            .query(self)
    }

    /// XGROUP CREATE <key> <groupname> <id or $> [MKSTREAM]
    #[inline]
    fn xgroup_create_mkstream<
        K: ToRedisArgs,
        G: ToRedisArgs,
        ID: ToRedisArgs,
        RV: FromRedisValue,
    >(
        &mut self,
        key: K,
        group: G,
        id: ID,
    ) -> RedisResult<RV> {
        cmd("XGROUP")
            .arg("CREATE")
            .arg(key)
            .arg(group)
            .arg(id)
            .arg("MKSTREAM")
            .query(self)
    }

    /// XGROUP SETID <key> <groupname> <id or $>
    #[inline]
    fn xgroup_setid<K: ToRedisArgs, G: ToRedisArgs, ID: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        group: G,
        id: ID,
    ) -> RedisResult<RV> {
        cmd("XGROUP")
            .arg("SETID")
            .arg(key)
            .arg(group)
            .arg(id)
            .query(self)
    }

    /// XGROUP DESTROY <key> <groupname>
    #[inline]
    fn xgroup_destroy<K: ToRedisArgs, G: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        group: G,
    ) -> RedisResult<RV> {
        cmd("XGROUP").arg("DESTROY").arg(key).arg(group).query(self)
    }

    /// XGROUP DELCONSUMER <key> <groupname> <consumername> */
    #[inline]
    fn xgroup_delconsumer<K: ToRedisArgs, G: ToRedisArgs, C: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        group: G,
        consumer: C,
    ) -> RedisResult<RV> {
        cmd("XGROUP")
            .arg("DELCONSUMER")
            .arg(key)
            .arg(group)
            .arg(consumer)
            .query(self)
    }

    /// XINFO CONSUMERS <key> <group>
    #[inline]
    fn xinfo_consumers<K: ToRedisArgs, G: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        group: G,
    ) -> RedisResult<RV> {
        cmd("XINFO")
            .arg("CONSUMERS")
            .arg(key)
            .arg(group)
            .query(self)
    }

    /// XINFO GROUPS <key>
    #[inline]
    fn xinfo_groups<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> RedisResult<RV> {
        cmd("XINFO").arg("GROUPS").arg(key).query(self)
    }

    /// XINFO STREAM <key>
    #[inline]
    fn xinfo_stream<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> RedisResult<RV> {
        cmd("XINFO").arg("STREAM").arg(key).query(self)
    }

    /// XLEN <key>
    #[inline]
    fn xlen<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> RedisResult<RV> {
        cmd("XLEN").arg(key).query(self)
    }

    /// XPENDING <key> <group> [<start> <stop> <count> [<consumer>]]
    #[inline]
    fn xpending<K: ToRedisArgs, G: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        group: G,
    ) -> RedisResult<RV> {
        cmd("XPENDING").arg(key).arg(group).query(self)
    }

    /// XPENDING <key> <group> <start> <stop> <count>
    #[inline]
    fn xpending_count<
        K: ToRedisArgs,
        G: ToRedisArgs,
        S: ToRedisArgs,
        E: ToRedisArgs,
        C: ToRedisArgs,
        RV: FromRedisValue,
    >(
        &mut self,
        key: K,
        group: G,
        start: S,
        end: E,
        count: C,
    ) -> RedisResult<RV> {
        cmd("XPENDING")
            .arg(key)
            .arg(group)
            .arg(start)
            .arg(end)
            .arg(count)
            .query(self)
    }

    /// XPENDING <key> <group> <start> <stop> <count> <consumer>
    #[inline]
    fn xpending_consumer_count<
        K: ToRedisArgs,
        G: ToRedisArgs,
        S: ToRedisArgs,
        E: ToRedisArgs,
        C: ToRedisArgs,
        CN: ToRedisArgs,
        RV: FromRedisValue,
    >(
        &mut self,
        key: K,
        group: G,
        start: S,
        end: E,
        count: C,
        consumer: CN,
    ) -> RedisResult<RV> {
        cmd("XPENDING")
            .arg(key)
            .arg(group)
            .arg(start)
            .arg(end)
            .arg(count)
            .arg(consumer)
            .query(self)
    }

    /// XRANGE key start end
    #[inline]
    fn xrange<K: ToRedisArgs, S: ToRedisArgs, E: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        start: S,
        end: E,
    ) -> RedisResult<RV> {
        cmd("XRANGE").arg(key).arg(start).arg(end).query(self)
    }

    /// XRANGE key - +
    #[inline]
    fn xrange_all<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> RedisResult<RV> {
        cmd("XRANGE").arg(key).arg("-").arg("+").query(self)
    }

    /// XRANGE key start end [COUNT <n>]
    #[inline]
    fn xrange_count<
        K: ToRedisArgs,
        S: ToRedisArgs,
        E: ToRedisArgs,
        C: ToRedisArgs,
        RV: FromRedisValue,
    >(
        &mut self,
        key: K,
        start: S,
        end: E,
        count: C,
    ) -> RedisResult<RV> {
        cmd("XRANGE")
            .arg(key)
            .arg(start)
            .arg(end)
            .arg("COUNT")
            .arg(count)
            .query(self)
    }

    /// XREAD STREAMS key_1 key_2 ... key_N ID_1 ID_2 ... ID_N
    #[inline]
    fn xread<K: ToRedisArgs, ID: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        keys: &[K],
        ids: &[ID],
    ) -> RedisResult<RV> {
        cmd("XREAD").arg("STREAMS").arg(keys).arg(ids).query(self)
    }

    /// XREAD [BLOCK <milliseconds>] [COUNT <count>] STREAMS key_1 key_2 ... key_N
    ///       ID_1 ID_2 ... ID_N
    /// XREADGROUP [BLOCK <milliseconds>] [COUNT <count>] [GROUP group-name consumer-name] STREAMS key_1 key_2 ... key_N
    ///       ID_1 ID_2 ... ID_N
    #[inline]
    fn xread_options<K: ToRedisArgs, ID: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        keys: &[K],
        ids: &[ID],
        options: StreamReadOptions,
    ) -> RedisResult<RV> {
        cmd(if options.read_only() {
            "XREAD"
        } else {
            "XREADGROUP"
        })
        .arg(options)
        .arg("STREAMS")
        .arg(keys)
        .arg(ids)
        .query(self)
    }

    /// XREVRANGE key end start
    #[inline]
    fn xrevrange<K: ToRedisArgs, E: ToRedisArgs, S: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        end: E,
        start: S,
    ) -> RedisResult<RV> {
        cmd("XREVRANGE").arg(key).arg(end).arg(start).query(self)
    }

    /// XREVRANGE key + -
    fn xrevrange_all<K: ToRedisArgs, RV: FromRedisValue>(&mut self, key: K) -> RedisResult<RV> {
        cmd("XREVRANGE").arg(key).arg("+").arg("-").query(self)
    }

    /// XREVRANGE key end start [COUNT <n>]
    #[inline]
    fn xrevrange_count<
        K: ToRedisArgs,
        E: ToRedisArgs,
        S: ToRedisArgs,
        C: ToRedisArgs,
        RV: FromRedisValue,
    >(
        &mut self,
        key: K,
        end: E,
        start: S,
        count: C,
    ) -> RedisResult<RV> {
        cmd("XREVRANGE")
            .arg(key)
            .arg(end)
            .arg(start)
            .arg("COUNT")
            .arg(count)
            .query(self)
    }

    /// XTRIM <key> MAXLEN [~|=] <count>  (like XADD MAXLEN option)
    #[inline]
    fn xtrim<K: ToRedisArgs, RV: FromRedisValue>(
        &mut self,
        key: K,
        maxlen: StreamMaxlen,
    ) -> RedisResult<RV> {
        cmd("XTRIM").arg(key).arg(maxlen).query(self)
    }
}

impl<T> StreamCommands for T where T: ConnectionLike {}
