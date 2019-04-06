use redis::{from_redis_value, FromRedisValue, RedisResult, RedisWrite, ToRedisArgs, Value};

use std::collections::HashMap;
//use std::hash::{BuildHasher, Hash};

// Stream Maxlen Enum

#[derive(PartialEq, Eq, Clone, Debug, Copy)]
pub enum StreamMaxlen {
    Equals(usize),
    Aprrox(usize),
}

impl ToRedisArgs for StreamMaxlen {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let (ch, val) = match *self {
            StreamMaxlen::Equals(v) => ("=", v),
            StreamMaxlen::Aprrox(v) => ("~", v),
        };
        out.write_arg("MAXLEN".as_bytes());
        out.write_arg(ch.as_bytes());
        out.write_arg(format!("{}", val).as_bytes());
    }
}

#[derive(Default, Debug)]
pub struct StreamClaimOptions {
    idle: Option<usize>,
    time: Option<usize>,
    retry: Option<usize>,
    force: bool,
    justid: bool,
}

impl StreamClaimOptions {
    pub fn idle(mut self, ms: usize) -> Self {
        self.idle = Some(ms);
        self
    }

    pub fn time(mut self, ms_time: usize) -> Self {
        self.time = Some(ms_time);
        self
    }

    pub fn retry(mut self, count: usize) -> Self {
        self.retry = Some(count);
        self
    }

    pub fn with_force(mut self) -> Self {
        self.force = true;
        self
    }

    pub fn with_justid(mut self) -> Self {
        self.justid = true;
        self
    }
}

impl ToRedisArgs for StreamClaimOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(ref ms) = self.idle {
            out.write_arg("IDLE".as_bytes());
            out.write_arg(format!("{}", ms).as_bytes());
        }
        if let Some(ref ms_time) = self.time {
            out.write_arg("TIME".as_bytes());
            out.write_arg(format!("{}", ms_time).as_bytes());
        }
        if let Some(ref count) = self.retry {
            out.write_arg("RETRYCOUNT".as_bytes());
            out.write_arg(format!("{}", count).as_bytes());
        }
        if self.force {
            out.write_arg("FORCE".as_bytes());
        }
        if self.justid {
            out.write_arg("JUSTID".as_bytes());
        }
    }
}

/// XREAD [BLOCK <milliseconds>] [COUNT <count>] STREAMS key_1 key_2 ... key_N
///       ID_1 ID_2 ... ID_N

/// XREADGROUP [BLOCK <milliseconds>] [COUNT <count>]
///            [GROUP group-name consumer-name] STREAMS key_1 key_2 ... key_N
///            ID_1 ID_2 ... ID_N

#[derive(Default, Debug)]
pub struct StreamReadOptions {
    block: Option<usize>,
    count: Option<usize>,
    group: Option<(Vec<Vec<u8>>, Vec<Vec<u8>>)>,
}

impl StreamReadOptions {
    pub fn read_only(&self) -> bool {
        self.group.is_none()
    }

    pub fn block(mut self, ms: usize) -> Self {
        self.block = Some(ms);
        self
    }

    pub fn count(mut self, n: usize) -> Self {
        self.count = Some(n);
        self
    }

    pub fn group<GN: ToRedisArgs, CN: ToRedisArgs>(
        mut self,
        group_name: GN,
        consumer_name: CN,
    ) -> Self {
        self.group = Some((
            ToRedisArgs::to_redis_args(&group_name),
            ToRedisArgs::to_redis_args(&consumer_name),
        ));
        self
    }
}

impl ToRedisArgs for StreamReadOptions {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        if let Some(ref ms) = self.block {
            out.write_arg("BLOCK".as_bytes());
            out.write_arg(format!("{}", ms).as_bytes());
        }

        if let Some(ref n) = self.count {
            out.write_arg("COUNT".as_bytes());
            out.write_arg(format!("{}", n).as_bytes());
        }

        if let Some(ref group) = self.group {
            out.write_arg("GROUP".as_bytes());
            for i in &group.0 {
                out.write_arg(i);
            }
            for i in &group.1 {
                out.write_arg(i);
            }
        }
    }
}

#[derive(Default, Debug)]
pub struct StreamReadReply {
    pub keys: Vec<StreamKey>,
}

#[derive(Default, Debug)]
pub struct StreamRangeReply {
    pub ids: Vec<StreamId>,
}

#[derive(Default, Debug)]
pub struct StreamClaimReply {
    pub ids: Vec<StreamId>,
}

#[derive(Default, Debug)]
pub struct StreamPendingReply {
    pub count: usize,
    pub start_id: String,
    pub end_id: String,
    pub consumers: Vec<StreamInfoConsumer>,
}

#[derive(Default, Debug)]
pub struct StreamPendingCountReply {
    pub ids: Vec<StreamPendingId>,
}

#[derive(Default, Debug)]
pub struct StreamInfoStreamsReply {
    pub last_generated_id: String,
    pub radix_tree_keys: usize,
    pub groups: usize,
    pub length: usize,
    pub first_entry: StreamId,
    pub last_entry: StreamId,
}

#[derive(Default, Debug)]
pub struct StreamInfoConsumersReply {
    pub consumers: Vec<StreamInfoConsumer>,
}

#[derive(Default, Debug)]
pub struct StreamInfoGroupsReply {
    pub groups: Vec<StreamInfoGroup>,
}

#[derive(Default, Debug)]
pub struct StreamInfoConsumer {
    pub name: String,
    pub pending: usize,
    pub idle: usize,
}

#[derive(Default, Debug)]
pub struct StreamInfoGroup {
    pub name: String,
    pub consumers: usize,
    pub pending: usize,
    pub last_delivered_id: String,
}

#[derive(Default, Debug)]
pub struct StreamPendingId {
    pub id: String,
    pub consumer: String,
    pub last_delivered_ms: usize,
    pub times_delivered: usize,
}

#[derive(Default, Debug)]
pub struct StreamKey {
    pub key: String,
    pub ids: Vec<StreamId>,
}

impl StreamKey {
    pub fn just_ids(&self) -> Vec<&String> {
        self.ids.iter().map(|msg| &msg.id).collect::<Vec<&String>>()
    }
}

#[derive(Default, Debug)]
pub struct StreamId {
    pub id: String,
    pub map: HashMap<String, Value>,
}

impl StreamId {
    pub fn from_bulk_value(v: &Value) -> RedisResult<Self> {
        let mut stream_id = StreamId::default();
        match *v {
            Value::Bulk(ref values) => {
                if values.len() >= 1 {
                    stream_id.id = from_redis_value(&values[0])?;
                }
                if values.len() >= 2 {
                    stream_id.map = from_redis_value(&values[1])?;
                }
            }
            _ => {}
        }

        Ok(stream_id)
    }

    pub fn get<T: FromRedisValue>(&self, key: &str) -> Option<T> {
        match self.find(&key) {
            Some(ref x) => from_redis_value(*x).ok(),
            None => None,
        }
    }

    pub fn find(&self, key: &&str) -> Option<&Value> {
        self.map.get(*key)
    }

    pub fn contains_key(&self, key: &&str) -> bool {
        self.find(key).is_some()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}

impl FromRedisValue for StreamReadReply {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let rows: Vec<HashMap<String, Vec<HashMap<String, HashMap<String, Value>>>>> =
            from_redis_value(v)?;
        let mut reply = StreamReadReply::default();
        for row in &rows {
            for (key, entry) in row.iter() {
                let mut k = StreamKey::default();
                k.key = key.to_owned();
                for id_row in entry {
                    let mut i = StreamId::default();
                    for (id, map) in id_row.iter() {
                        i.id = id.to_owned();
                        i.map = map.to_owned();
                    }
                    k.ids.push(i);
                }
                reply.keys.push(k);
            }
        }
        Ok(reply)
    }
}

impl FromRedisValue for StreamRangeReply {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let rows: Vec<HashMap<String, HashMap<String, Value>>> = from_redis_value(v)?;
        let mut reply = StreamRangeReply::default();
        for row in &rows {
            let mut i = StreamId::default();
            for (id, map) in row.iter() {
                i.id = id.to_owned();
                i.map = map.to_owned();
            }
            reply.ids.push(i);
        }
        Ok(reply)
    }
}

impl FromRedisValue for StreamClaimReply {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let rows: Vec<HashMap<String, HashMap<String, Value>>> = from_redis_value(v)?;
        let mut reply = StreamClaimReply::default();
        for row in &rows {
            let mut i = StreamId::default();
            for (id, map) in row.iter() {
                i.id = id.to_owned();
                i.map = map.to_owned();
            }
            reply.ids.push(i);
        }
        Ok(reply)
    }
}

impl FromRedisValue for StreamPendingReply {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let parts: (usize, String, String, Vec<Vec<String>>) = from_redis_value(v)?;
        let mut reply = StreamPendingReply::default();
        reply.count = parts.0.to_owned() as usize;
        reply.start_id = parts.1.to_owned();
        reply.end_id = parts.2.to_owned();
        for consumer in &parts.3 {
            let mut info = StreamInfoConsumer::default();
            info.name = consumer[0].to_owned();
            if let Ok(v) = consumer[1].to_owned().parse::<usize>() {
                info.pending = v;
            }
            reply.consumers.push(info);
        }
        Ok(reply)
    }
}

impl FromRedisValue for StreamPendingCountReply {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let parts: Vec<Vec<(String, String, usize, usize)>> = from_redis_value(v)?;
        let mut reply = StreamPendingCountReply::default();
        for row in &parts {
            let mut p = StreamPendingId::default();
            p.id = row[0].0.to_owned();
            p.consumer = row[0].1.to_owned();
            p.last_delivered_ms = row[0].2.to_owned();
            p.times_delivered = row[0].3.to_owned();
            reply.ids.push(p);
        }
        Ok(reply)
    }
}

impl FromRedisValue for StreamInfoStreamsReply {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let map: HashMap<String, Value> = from_redis_value(v)?;
        let mut reply = StreamInfoStreamsReply::default();
        if let Some(v) = &map.get("last-generated-id") {
            reply.last_generated_id = from_redis_value(v)?;
        }
        if let Some(v) = &map.get("radix-tree-nodes") {
            reply.radix_tree_keys = from_redis_value(v)?;
        }
        if let Some(v) = &map.get("groups") {
            reply.groups = from_redis_value(v)?;
        }
        if let Some(v) = &map.get("length") {
            reply.length = from_redis_value(v)?;
        }
        if let Some(v) = &map.get("first-entry") {
            reply.first_entry = StreamId::from_bulk_value(v)?;
        }
        if let Some(v) = &map.get("last-entry") {
            reply.last_entry = StreamId::from_bulk_value(v)?;
        }
        Ok(reply)
    }
}

impl FromRedisValue for StreamInfoConsumersReply {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let consumers: Vec<HashMap<String, Value>> = from_redis_value(v)?;
        let mut reply = StreamInfoConsumersReply::default();
        for map in consumers {
            let mut c = StreamInfoConsumer::default();
            if let Some(v) = &map.get("name") {
                c.name = from_redis_value(v)?;
            }
            if let Some(v) = &map.get("pending") {
                c.pending = from_redis_value(v)?;
            }
            if let Some(v) = &map.get("idle") {
                c.idle = from_redis_value(v)?;
            }
            reply.consumers.push(c);
        }

        Ok(reply)
    }
}

impl FromRedisValue for StreamInfoGroupsReply {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let groups: Vec<HashMap<String, Value>> = from_redis_value(v)?;
        let mut reply = StreamInfoGroupsReply::default();
        for map in groups {
            let mut g = StreamInfoGroup::default();
            if let Some(v) = &map.get("name") {
                g.name = from_redis_value(v)?;
            }
            if let Some(v) = &map.get("pending") {
                g.pending = from_redis_value(v)?;
            }
            if let Some(v) = &map.get("consumers") {
                g.consumers = from_redis_value(v)?;
            }
            if let Some(v) = &map.get("last-delivered-id") {
                g.last_delivered_id = from_redis_value(v)?;
            }
            reply.groups.push(g);
        }
        Ok(reply)
    }
}
