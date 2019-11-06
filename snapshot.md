# 快照说明

## 1、创建快照
举例来说，在创建磁盘disktest的时候，默认会在指定的存储池下生成磁盘disktest, 这个时候只有一个磁盘没有快照。disktest作为云盘的current。

| disktest |

这是创建快照disktest.1，快照链结果如下：

| disktest | <—— | disktest.1 |

disktest实际上作为快照保存上一时刻的状态，这时disktest.1会作为云盘的current使用。再次创建快照disktest.2，快照链结果如下：


| disktest | <—— | disktest.1 | <—— | disktest.2 |

disktest.1实际上作为快照保存上一时刻的状态，这时disktest.2作为current也就是云盘使用。

## 2、恢复快照
假设现在有快照链如下，disktest.3作为current，也就是云盘使用。

| disktest | <—— | disktest.1 | <—— | disktest.2 | <—— | disktest.3 |

这时候恢复current（也就是云盘）到快照disktest.1时的状态，快照链结果如下：

| disktest | <—— | disktest.1 | <—— | disktest.2 | <—— | disktest.3 |

云盘会恢复到disktest.1时的状态，这时候uuid作为current，原来的快照链不变。

## 3、删除快照

删除快照分为两种情况：

删除current

删除分支节点


分支
