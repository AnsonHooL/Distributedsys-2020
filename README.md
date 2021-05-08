# Distributedsys-2020
#### lab version：6.824-2020

Lab2的代码在raft分支

Lab3A的代码在kvstore分支

Lab3B的代码在kvstore1.0分支

#### 进度

Lab1(√)

Lab2A(√)

Lab2B(√)

Lab2C(√)

2021/04/22：

批量测试了2000次均通过，正确性应该没问题 (结果在doc文件夹里)

脚本位置在src/ratf/go-test-many.sh中，是6.824助教提供的。

2021/04/24：

Lab3A(√)

2021/04/29：

Lab3B(√)

```c++
Test: one client (3A) ...
  ... Passed --  15.1  5  6220 1234
Test: many clients (3A) ...
  ... Passed --  15.4  5  6770 2931
Test: unreliable net, many clients (3A) ...
  ... Passed --  16.8  5  5817 1064
Test: concurrent append to same key, unreliable (3A) ...
  ... Passed --   1.3  3   175   52
Test: progress in majority (3A) ...
  ... Passed --   0.4  5    37    2
Test: no progress in minority (3A) ...
  ... Passed --   1.0  5   106    3
Test: completion after heal (3A) ...
  ... Passed --   1.1  5    44    3
Test: partitions, one client (3A) ...
  ... Passed --  22.5  5  6539 1182
Test: partitions, many clients (3A) ...
  ... Passed --  22.7  5  7705 2714
Test: restarts, one client (3A) ...
  ... Passed --  19.7  5  6454 1265
Test: restarts, many clients (3A) ...
  ... Passed --  20.2  5  7492 2989
Test: unreliable net, restarts, many clients (3A) ...
  ... Passed --  20.8  5  6265 1036
Test: restarts, partitions, many clients (3A) ...
  ... Passed --  27.6  5  8008 2862
Test: unreliable net, restarts, partitions, many clients (3A) ...
  ... Passed --  28.8  5  5691  704
Test: unreliable net, restarts, partitions, many clients, linearizability checks (3A) ...
  ... Passed --  25.6  7 14546 1517
Test: InstallSnapshot RPC (3B) ...
  ... Passed --   2.8  3   275   63
Test: snapshot size is reasonable (3B) ...
  ... Passed --   6.6  3  2430  800
Test: restarts, snapshots, one client (3B) ...
  ... Passed --  20.7  5  9743 1891
Test: restarts, snapshots, many clients (3B) ...
  ... Passed --  20.0  5 30771 20209
Test: unreliable net, snapshots, many clients (3B) ...
  ... Passed --  16.8  5  5752 1039
Test: unreliable net, restarts, snapshots, many clients (3B) ...
  ... Passed --  20.9  5  6340 1065
Test: unreliable net, restarts, partitions, snapshots, many clients (3B) ...
  ... Passed --  28.6  5  6010  839
Test: unreliable net, restarts, partitions, snapshots, many clients, linearizability checks (3B) ...
  ... Passed --  25.6  7 14706 1515
PASS
ok  	_/home/thl/MIT-6.824-skeleton/src/kvraft	381.930s

real	6m22.367s
user	6m17.696s
sys 	0m47.163s
```

2021/05/08 ：

修复lab3B的bug，raft持久化恢复时，应该先初始化数据，再读persistent，但是我先读了persistent，再初始化第0个位置的日志，使得错误的结点当选Leader。

3B跑了200次，单个TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B跑了1000次



TODO：

lab4

Challenge