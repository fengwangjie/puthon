ROOMIS Integration
------------------

1.  luigi的使用

   Target
   Target可对应为磁盘上的文件，或HDFS上文件,或checkpoint点，或数据库等。对于Target来说，唯一需要实现的方法为exists,返回为True表示存在，否则不存在返回为False.