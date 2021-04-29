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

批量测试了2000次均通过，正确性应该没问题，下面是测试结果 : )

脚本位置在src/ratf/go-test-many.sh中，是6.824助教提供的。

![lab2测试](https://github.com/AnsonHooL/Distributedsys-2020/blob/kvstore1.0/src/doc/lab2%E6%B5%8B%E8%AF%95.jpg)

2021/04/24：

Lab3A(√)

2021/04/29：

Lab3B(√)

```C
Test: one client (3A) ...
  ... Passed --  15.1  5  6228 1249
Test: many clients (3A) ...
  ... Passed --  15.3  5  6681 2835
Test: unreliable net, many clients (3A) ...
  ... Passed --  16.4  5  5585  974
Test: concurrent append to same key, unreliable (3A) ...
  ... Passed --   1.1  3   180   52
Test: progress in majority (3A) ...
  ... Passed --   0.4  5    34    2
Test: no progress in minority (3A) ...
  ... Passed --   1.0  5   114    3
Test: completion after heal (3A) ...
  ... Passed --   1.1  5    49    3
Test: partitions, one client (3A) ...
  ... Passed --  22.5  5  6178 1087
Test: partitions, many clients (3A) ...
  ... Passed --  23.0  5  7881 3014
Test: restarts, one client (3A) ...
  ... Passed --  19.5  5  6354 1255
Test: restarts, many clients (3A) ...
  ... Passed --  19.7  5  7384 2970
Test: unreliable net, restarts, many clients (3A) ...
  ... Passed --  21.7  5  6287 1021
Test: restarts, partitions, many clients (3A) ...
  ... Passed --  27.3  5  7751 2847
Test: unreliable net, restarts, partitions, many clients (3A) ...
  ... Passed --  29.6  5  6003  793
Test: unreliable net, restarts, partitions, many clients, linearizability checks (3A) ...
  ... Passed --  25.1  7 14541 1598
Test: InstallSnapshot RPC (3B) ...
  ... Passed --   3.1  3   278   63
Test: snapshot size is reasonable (3B) ...
  ... Passed --   6.5  3  2421  800
Test: restarts, snapshots, one client (3B) ...
  ... Passed --  20.3  5  9706 1890
Test: restarts, snapshots, many clients (3B) ...
  ... Passed --  20.2  5 29448 18722
Test: unreliable net, snapshots, many clients (3B) ...
  ... Passed --  17.3  5  5795 1015
Test: unreliable net, restarts, snapshots, many clients (3B) ...
  ... Passed --  21.2  5  6610 1124
Test: unreliable net, restarts, partitions, snapshots, many clients (3B) ...
  ... Passed --  29.9  5  5930  792
Test: unreliable net, restarts, partitions, snapshots, many clients, linearizability checks (3B) ...
  ... Passed --  25.5  7 15882 1835
PASS
ok  	_/home/thl/MIT-6.824-skeleton/src/kvraft	143.999s
```





#### TODO:

Lab3B有平均500次出现的一次BUG，暂时没想法修复 ~