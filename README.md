TODO:
构建Trident topology，spout使用TransactionalTridentKafkaSpout，采用统计如下信息：
1)	将各销售商的总的totalPrice进行统计入库，使用事务持久化模型。
2)	每隔20s统计一次近1分钟内各商品销售的销售量，存入数据库或内存，支持DRPC查询。
