# Webcrawl

It is a simple tool for coder to develop executable .py file of grabbing web datas. 

## task scheduling

>    - workflow 
>    - priority 
>    - selfloop 
>    - subtask timeout 
>    - task timeout 

## response analysis

>    - html 
>    - xml 
>    - json 

## only redis queue task status (except of other queue)
```
 |-------put ---------- get             insert   insert
 |       /                \                |        |
 |     WAIT---[ready]--- RUNNING --------COMPLETED    |
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