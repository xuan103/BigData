# Class_ppt

- [大數據簡介](https://github.com/xuan103/BigData/wiki/%E5%A4%A7%E6%95%B8%E6%93%9A%E7%B0%A1%E4%BB%8B)

- [1. 認識 linux](https://github.com/xuan103/BigData/wiki/1.-%E8%AA%8D%E8%AD%98-linux)

- [2. Hadoop HDFS](https://github.com/xuan103/BigData/wiki/2.-Hadoop-HDFS)

- [3. Apache Pig 新手入門](https://github.com/xuan103/BigData/wiki/3.-Apache-Pig-%E6%96%B0%E6%89%8B%E5%85%A5%E9%96%80)

- [4. Apache Pig 駕輕就熟](https://github.com/xuan103/BigData/wiki/4.-Apache-Pig-%E9%A7%95%E8%BC%95%E5%B0%B1%E7%86%9F)


# 練習題匯總 :

* * * 

## 第一週練習

- 請找出桃園市的藥局



- 請問臺北市與新北市的藥局哪個多, 分別有幾間?



- 請問臺北市大安區的藥局總共有幾間 ?



- 請找出藥局的名稱,縣市,地區與聯絡電話



- 接續上題, 將結果儲存至 ~/wk/data 目錄中的 mydata.csv中



* * * 

## 第二週練習

- 找出臺北市兒童剩餘口罩剩餘最多藥局的前五個藥局



- 續上題，請問兒童口套數量第50名的藥局名稱?



- 找出臺北市藥局的成人口罩數量並存成新的檔案



- 請問哪一間藥局擁有最多的兒童口罩 ? 藥局代號與名稱 ? 擁有多少兒童口罩 ? 地址與聯絡方式  ?



- 請將桃園縣也替換為桃園市, 並且再次統計全台灣各縣市的醫事機構



 * * *

## 第三週練習

載入資料
```
data = LOAD '/dataset/pig04/twmask.csv' USING PigStorage(',') AS(
code : chararray,
name : chararray,
address : chararray,
tel : chararray,
adult_mask : int,
child_mask : int,
update_time : chararray
);
```

- 找出各縣市的兒童口罩數量前三名

    - 使用 address (地址) 找出縣市名稱並以此來分組
      
      > gp_counties = GROUP data BY SUBSTRING($2,0,3);

    - 使用 SUM函數來統計每個群組(縣市)中的兒童口罩數量
      > sum_child = FOREACH gp_counties GENERATE $0,SUM($1.$5);

    - 使用 ORDER 來排序口罩數量 (降冪,倒排序,由大到小)
      > order_child = ORDER sum_child BY $1 DESC;

    - 使用 LIMIT 列出前三名
      > top3_child = LIMIT order_child 3;


- 找出臺北市的健康服務中心的成人口罩數量前三名

    - 使用 FILTER 來找出台北市與健康服務中心
      > filter_data = FILTER data BY SUBSTRING($2,0,3) == '臺北市' AND $1 MATCHES '.*健康服務中心';

    - 使用 ORDER 來排序成人口罩數量
      > order_adult = ORDER filter_data BY $4 DESC;

    - 使用 LIMIT 列出前三名
      > top3_adult = LIMIT order_adult 3;


- 找出臺北市各區域的兒童口罩數量前三名

    - 使用 FILTER 找出台北市的藥局資料
      > filter_data = FILTER data BY SUBSTRING($2,0,3) == '臺北市';

    - 使用 address 找出區域並以此來分組
      > gp_area = GROUP filter_data BY SUBSTRING($2,3,6);

    - 使用 SUM函數來統計每個群組(區域)中的兒童口罩數量
      > sum_child = FOREACH gp_area GENERATE $0,SUM($1.$5);

    - 使用 ORDER 來排序兒童口罩數量
      > order_child = ORDER sum_child BY $1 DESC;

    - 使用 LIMIT 列出前三名
      > top3_child = LIMIT order_child 3;

* * *

## 第四週練習

- 各縣市地區的成人與兒童口罩的總和最多的前三個, 各幾個 ?



- 找出臺北市的兒童口罩平均最多的前三個區域, 各幾個 ?



- 請找出全台灣人口最多的縣市



- 請找出臺北市各區域的口罩與人口的比例



* * *


