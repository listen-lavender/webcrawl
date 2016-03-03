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
 |     WAIT---READY--- RUNNING --------COMPLETED    |
 |                         |                        |
 |                         |                        |
RETRY----------------------|----------------------ERROR
 |                                                  |
 |                                                  |
 |__________________________________________________|

 WAIT     : 2
 READY    : 3
 RUNNING  : 4
 RETRY    : 5
 COMPLETED: 1
 ERROR    : 0
```