## BDP公共项目

此项目中包含FE，Tassadar，DI，OpenDS等项目会用到的公共代码，为了便于维护整体项目，公共代码应该置于此项目中。

公共项目的使用，应遵循以下原则：

- 只能单向引用此公共项目，避免互相应用
- 多个项目中同时用到的代码，一定要抽象到此公共项目中
- 此项目的代码，一定避免频繁改动接口（会影响其他项目，如有修改，必须通知到其他项目负责人，做好测试）

### 引用此项目

```
cd {PROJECT_HOME}
# 添加子项目，到src/common目录下（src/common目录初始应该不存在，可以根据项目需求调整目录）
git submodule add git@haizhi.me:bdp-server/bdp-common.git src/common
```

### 更新项目中对此项目的引用

更新此项目的代码到最新版本

```
cd src/common
git pull origin master
cd ../..
git commit -a "update bdp-common"
git push
```

项目中，更新对此项目的版本引用

```
cd {PROJECT_HOME}
git pull
git submodule update
```

初次将项目clone后，更新此项目

```
git submodule init
git submodule update
```

### 在项目中编辑此项目

```
cd src/common
git checkout master
# ... 修改代码
git commit -a
git push
```

更新对此项目的引用

```
cd {PROJECT_HOME}
git submodule update
cd src/common
git pull origin master
cd ../..
git commit -a "update bdp-common"
```

### 关于redis_helper
* 配置：
```language
 [redis]
 sentinel=test01
 cluster=172.16.34.50:6381,172.16.34.51:6381,172.16.34.141:6381
```

* 用法：
``` python
 from common.redis_helper import RedisHelper
    redis_helper = RedisHelper()
    redis_helper.set('test_key', 'test_value')
    redis_helper.get('test_key')
```

* 跟之前用法的区别
```txt
 之前的redis对set做了一个封装
 当把dict或list类型的数据传到set时会自动json.dumps
 在get的时候会自动json.loads
 但这样会有一个问题，当value为普通字符串的时候
 get的时候会有问题，所以这一版做了一个改动
 新增加了set_json和get_json两个方法，用来存储/获取对象
 set和get方法行为跟原生redis客户端相同
```