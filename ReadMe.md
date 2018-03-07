## Sino.Extensions.Dapper
用Dapper提供MySQL数据仓储的默认实现    

[![Build status](https://ci.appveyor.com/api/projects/status/o4y6ne8ix0tot0vq?svg=true)](https://ci.appveyor.com/project/vip56/sino-extensions-dapper)

## 使用方式
在`Startup`中进行配置，增加用于读和写的MySQL连接字符串
```
string readConStr = Configuration.GetConnectionString("ReadConnectionString");
string writeConStr = Configuration.GetConnectionString("WriteConnectionString");
services.AddDapper(writeConStr, readConStr);
```
  
为了保证了兼容性，所以`Connection`对象依然可以使用，但是对于特殊场景的使用可以直接使用`WriteConnection`或`ReadConnection`
来强制指定需要使用写数据库或读数据库。