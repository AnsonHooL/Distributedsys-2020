package shardmaster

import (
	"sort"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
	Timeout = "Timeout"
	WrongLeader = "WrongLeader"
)

type Err string
type msgId int64

type CommonArgs struct {
	MsgId    int64
	ClientId int64
}

type JoinArgs struct {
	CommonArgs
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	CommonArgs
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	CommonArgs
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	CommonArgs
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func (c *Config) Copy() Config {
	//var ss [NShards]int
	//for i, v := range c.Shards {
	//	ss[i] = v
	//}
	config := Config{
		Num:    c.Num,
		Shards: c.Shards,
		Groups: make(map[int][]string),
	}
	for gid, s := range c.Groups {
		config.Groups[gid] = append([]string{}, s...)
	}
	return config
}

//负载均衡
func adjustConfig(config *Config) {
	if len(config.Groups) == 0 {
		config.Shards = [NShards]int{}
	} else if len(config.Groups) == 1 {
		// set shards one gid
		//遍历map，将所有shard放到一个分区中
		for k, _ := range config.Groups {
			for i, _ := range config.Shards {
				config.Shards[i] = k
			}
		}
	} else if len(config.Groups) <= NShards {
		avg := NShards / len(config.Groups)
		// 每个 gid 分 avg 个 shard
		otherShardsCount := NShards - avg*len(config.Groups)
		needLoop := false
		lastGid := 0

	LOOP:
		//sm.log(fmt.Sprintf("config: %+v", config))
		var keys []int
		for k := range config.Groups {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		for _, gid := range keys {
			lastGid = gid
			count := 0
			// 先 count 已有的
			for _, val := range config.Shards {
				if val == gid {
					count += 1
				}
			}

			// 判断是否需要改变
			if count == avg { //已经有avg个分区，不需要改变
				continue
			} else if count > avg && otherShardsCount == 0 { //将大于avg的部分分区挪出来
				// 减少到 avg
				c := 0
				for i, val := range config.Shards {
					if val == gid {
						if c == avg {
							config.Shards[i] = 0
						} else {
							c += 1
						}
					}
				}

			} else if count > avg && otherShardsCount > 0 {
				// 减到 othersShardsCount 为 0
				// 若还 count > avg, set to 0
				c := 0
				for i, val := range config.Shards {
					if val == gid {
						if c == avg+otherShardsCount {
							config.Shards[i] = 0
						} else {
							if c == avg {
								otherShardsCount -= 1
							} else {
								c += 1
							}

						}
					}
				}

			} else {
				// count < avg, 此时有可能没有位置
				for i, val := range config.Shards {
					if count == avg {
						break
					}
					if val == 0 && count < avg {
						config.Shards[i] = gid
						count++ //thl fix a bug
					}
				}

				if count < avg {
					//sm.log(fmt.Sprintf("needLoop: %+v, %+v, %d ", config, otherShardsCount, gid))
					needLoop = true
				}

			}

		}

		if needLoop {
			needLoop = false
			goto LOOP
		}

		// 可能每一个 gid 都 >= avg，但此时有空的 shard
		if lastGid != 0 {
			for i, val := range config.Shards {
				if val == 0 {
					config.Shards[i] = lastGid
				}
			}
		}

	} else {
		// len(config.Groups) > NShards
		// 每个 gid 最多一个， 会有空余 gid
		gids := make(map[int]int)
		emptyShards := make([]int, 0, NShards)
		for i, gid := range config.Shards {
			if gid == 0 {
				emptyShards = append(emptyShards, i)
				continue
			}
			if _, ok := gids[gid]; ok {
				emptyShards = append(emptyShards, i)
				config.Shards[i] = 0
			} else {
				gids[gid] = 1
			}
		}
		n := 0
		if len(emptyShards) > 0 {
			var keys []int
			for k := range config.Groups {
				keys = append(keys, k)
			}
			sort.Ints(keys)
			for _, gid := range keys {
				if _, ok := gids[gid]; !ok {
					config.Shards[emptyShards[n]] = gid
					n += 1
				}
				if n >= len(emptyShards) {
					break
				}
			}
		}

	}

}