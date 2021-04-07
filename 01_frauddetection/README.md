####构建项目

```
mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-walkthrough-datastream-java \
    -DarchetypeVersion=1.12.0 \
    -DgroupId=frauddetection \
    -DartifactId=frauddetection \
    -Dversion=0.1 \
    -Dpackage=spendreport \
    -DinteractiveMode=false

```

#### 实现目标

欺诈检测器条件：
    出现小于 $1 美元的交易后紧跟着一个大于 $500 的交易；
    设置了一分钟的超时。
-> 输出报警信息

    
    
#### 日志分析
```
17:53:50,575 INFO  spendreport.FraudDetector                                    [] - Transaction{accountId=2, timestamp=1546297560000, amount=230.18}
17:53:50,674 INFO  spendreport.FraudDetector                                    [] - ========== processElement
17:53:50,675 INFO  spendreport.FraudDetector                                    [] - Transaction{accountId=3, timestamp=1546297920000, amount=0.8}
17:53:50,675 INFO  spendreport.FraudDetector                                    [] - ========== cleanUp
17:53:50,675 INFO  spendreport.FraudDetector                                    [] - ========== SMALL_AMOUNT update
17:53:50,775 INFO  spendreport.FraudDetector                                    [] - ========== processElement
17:53:50,775 INFO  spendreport.FraudDetector                                    [] - Transaction{accountId=4, timestamp=1546298280000, amount=350.89}
17:53:50,876 INFO  spendreport.FraudDetector                                    [] - ========== processElement
17:53:50,876 INFO  spendreport.FraudDetector                                    [] - Transaction{accountId=5, timestamp=1546298640000, amount=127.55}
17:53:50,975 INFO  spendreport.FraudDetector                                    [] - ========== processElement
17:53:50,975 INFO  spendreport.FraudDetector                                    [] - Transaction{accountId=1, timestamp=1546299000000, amount=483.91}
17:53:51,076 INFO  spendreport.FraudDetector                                    [] - ========== processElement
17:53:51,077 INFO  spendreport.FraudDetector                                    [] - Transaction{accountId=2, timestamp=1546299360000, amount=228.22}
17:53:51,176 INFO  spendreport.FraudDetector                                    [] - ========== processElement
17:53:51,176 INFO  spendreport.FraudDetector                                    [] - Transaction{accountId=3, timestamp=1546299720000, amount=871.15}
17:53:51,176 INFO  spendreport.FraudDetector                                    [] - ========== LARGE_AMOUNT alert
17:53:51,176 INFO  org.apache.flink.walkthrough.common.sink.AlertSink           [] - Alert{id=3}
17:53:51,177 INFO  spendreport.FraudDetector                                    [] - ========== cleanUp
17:53:51,277 INFO  spendreport.FraudDetector                                    [] - ========== processElement

```
    
    
    
    
    
    
    
    
    
    