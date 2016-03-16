# webcrawl
[![Build Status](https://api.travis-ci.org/listen-lavender/webcrawl.svg?branch=master)](https://api.travis-ci.org/listen-lavender/webcrawl)
webcrawl是对抓取常用工具的封装，包括requests，lxml，phantomjs，并且实现了workflow，使coder在遵守规范的基础上更专注抓取业务，方便快速实现稳定的工程；还有一些其他会用到的工具的封装，例如rsa.py是http://www.ohdave.com/rsa/的Python版本，这个很多网站有用到；atlas.py设计到一些地图坐标的处理。

## http请求增强
handleRequest.py是对requests模块抓取常用的http方法以及lxml解析的封装，以及phantomsjs代理的支持，还有一些通用内容的处理
>    - html 
>    - xml 
>    - json 
>    - text 
>    - response object 

## task的简单控制
task.py(work.py)是任务流workflow的实现，是数据驱动异步执行的，类似于celery的chain，group，chord等的复合类型，但是比celery的这方面更强大更好用，并且控制着抓取代码的编写规范，依赖于pjq队列
>    - workflow 
>    - priority 
>    - selfloop 
>    - subtask timeout 
>    - task timeout 

## queue支持
pjq.py是priority join queue，为了支持任务流的实现，其中redis queue比较强大，支持task的增查改，就是在执行过程中subtask是可控的。
>    - workflow 
>    - priority 
>    - selfloop 
>    - subtask timeout 
>    - task timeout 

## redis queue
```
 |-------put ---------- get             insert   insert
 |       /                \                |        |
 |     WAIT---[ready]--- RUNNING --------COMPLETED  |
 |                         |                        |
 |                         |                        |
RETRY----------------------|----------------------ERROR
 |                                                  |
 |                                                  |
 |__________________________________________________|

 WAIT     : 2
 RUNNING  : 3
 RETRY    : 4
 ABANDONED: 5
 COMPLETED: 1
 ERROR    : 0
 ready   - 10
```