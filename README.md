# Class_ppt

- [大數據簡介](https://github.com/xuan103/BigData/wiki/%E5%A4%A7%E6%95%B8%E6%93%9A%E7%B0%A1%E4%BB%8B)

- [1. 認識 linux](https://github.com/xuan103/BigData/wiki/1.-%E8%AA%8D%E8%AD%98-linux)

- [2. Hadoop HDFS](https://github.com/xuan103/BigData/wiki/2.-Hadoop-HDFS)

- [3. Apache Pig 新手入門](https://github.com/xuan103/BigData/wiki/3.-Apache-Pig-%E6%96%B0%E6%89%8B%E5%85%A5%E9%96%80)

- [4. Apache Pig 駕輕就熟](https://github.com/xuan103/BigData/wiki/4.-Apache-Pig-%E9%A7%95%E8%BC%95%E5%B0%B1%E7%86%9F)


# 練習題匯總 :

* * * 

## 第一週練習

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

- 請找出桃園市的藥局

    - bash
    
      > cat twmask.csv | grep -e '桃園市' 
    
    ```
    ..
    5932124265,弘宗藥局,桃園市觀音區中山路１５號,(03)4735659,2600,66,2020/10/31 00:47:59
    5932124345,甘泉藥局,桃園市觀音區中正路２４２號,(03)4738659,27,0,2020/10/31 00:47:59
    5932124372,福星藥局,桃園市觀音區大觀路二段２１３號１樓,(03)4389138,2970,58,2020/10/31 00:47:59
    5932124381,日昇藥局,桃園市觀音區成功路一段７８１號,(03)4834212,2538,460,2020/10/31 00:47:59
    5932130049,德怡藥局,桃園市復興區澤仁里忠孝路３４號,(03)3821686,624,638,2020/10/31 00:47:59
    ```
    
    - pig
      
      >> gp_counties = GROUP data BY SUBSTRING($2,0,3);
    
      >> filter_data = FILTER data BY SUBSTRING($2,0,3) == '桃園市';
    
      >>  dump filter_data
      
    ```
    ..
    (5932124265,弘宗藥局,桃園市觀音區中山路１５號,(03)4735659,2600,66,2020/10/31 00:47:59)
    (5932124345,甘泉藥局,桃園市觀音區中正路２４２號,(03)4738659,27,0,2020/10/31 00:47:59)
    (5932124372,福星藥局,桃園市觀音區大觀路二段２１３號１樓,(03)4389138,2970,58,2020/10/31 00:47:59)
    (5932124381,日昇藥局,桃園市觀音區成功路一段７８１號,(03)4834212,2538,460,2020/10/31 00:47:59)
    (5932130049,德怡藥局,桃園市復興區澤仁里忠孝路３４號,(03)3821686,624,638,2020/10/31 00:47:59)
    ```

- 請問臺北市與新北市的藥局哪個多, 分別有幾間?

    - bash
    
      > cat twmask.csv | grep -e '臺北市' | wc -l
    
      > cat twmask.csv | grep -e '新北市' | wc -l 

    - pig

      >> taipei = FOREACH data GENERATE address;

      >> taipei1 = FILTER taipei BY $0 matches '臺北市.*' or $0 matches '新北市.*';
   
      >> taipei2 = FOREACH taipei1 GENERATE $0, SUBSTRING($0,0,3);

      >> taipei3 = GROUP taipei2 BY $1;

      >> taipei4 = FOREACH taipei3 GENERATE COUNT($1),$0;
    
      >>  dump taipei4
    
    ```
    (970,新北市)
    (614,臺北市)
    ```

- 請問臺北市大安區的藥局總共有幾間 ?

    - bash
    
      > cat twmask.csv | grep -e '臺北市大安區' | wc -l
      
    ```
    74
    ```
      
    - pig

      >> taipei = FOREACH data GENERATE address;

      >> taipei1 = FILTER taipei BY $0 matches '臺北市大安區.*';

      >> taipei2 = FOREACH taipei1 GENERATE $0, SUBSTRING($0,0,3);

      >> taipei3 = GROUP taipei2 BY $1;

      >> taipei4 = FOREACH taipei3 GENERATE COUNT($1),$0;
     
      >> dump taipei4

    ```
    (74,臺北市)
    ```

- 請找出藥局的名稱,縣市,地區與聯絡電話

    - bash
    
      > cat twmask.csv | cut -d , -f 2,3,4,8
      
    ```
    大森藥局,金門縣金城鎮民生路２８、３０號１、２樓,(82)325100
    百泰藥局,金門縣金城鎮西海路１段１號,(082)312832
    大金藥局,金門縣金沙鎮汶沙里五福街１號,(082)355382
    仁愛復興藥局,金門縣金湖鎮新市里復興路４０號,(082)332368
    大山藥局,金門縣金湖鎮新市里中正路２號、２－２號,(082)333290
    ```

    - pig
    
      >> dump data
      
    ```
    (5990010631,大森藥局,金門縣金城鎮民生路２８、３０號１、２樓,(82)325100,3132,800,2020/10/31 00:47:59)
    (5990010668,百泰藥局,金門縣金城鎮西海路１段１號,(082)312832,2706,860,2020/10/31 00:47:59)
    (5990020020,大金藥局,金門縣金沙鎮汶沙里五福街１號,(082)355382,1618,1100,2020/10/31 00:47:59)
    (5990030044,仁愛復興藥局,金門縣金湖鎮新市里復興路４０號,(082)332368,1234,248,2020/10/31 00:47:59)
    (5990030062,大山藥局,金門縣金湖鎮新市里中正路２號、２－２號,(082)333290,3083,279,2020/10/31 00:47:59)
    ```

- 接續上題, 將結果儲存至 ~/wk/data 目錄中的 mydata.csv中

    - bash
    
      > mkdir wk
      
      > cd wk
      
      > mkdir data
      
      > cd ..
      
      > mv twmask.csv wk/data/mydata.csv
      

* * * 

## 第二週練習

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

- 找出臺北市兒童剩餘口罩剩餘最多藥局的前五個藥局

    - bash
    
      >  cat twmask.csv | sort -t',' -k6 | cut -d',' -f2 | head -n 5
      
      ```
      新竹縣新豐鄉衛生所
      宜蘭縣頭城鎮衛生所
      苗栗縣公館鄉衛生所
      松田藥局
      阿里藥局
      ```
      
- 續上題，請問兒童口套數量第50名的藥局名稱?

    - bash
    
      >  cat twmask.csv | sort -t',' -k6 | cut -d',' -f2 | head -n 50 | cat -n
      
      ```
      ..
      46	大賀藥局
      47	幸一藥局
      48	京元藥局
      49	楊梅丁丁藥局
      50	大吉藥局
      ```

- 找出臺北市藥局的成人口罩數量並存成新的檔案

    - bash
    
      > cat twmask.csv | sort -r -t',' -k5 | cut -d',' -f2,3,5 > twmasknew.csv
      > ll
      
      ```
      ..
      -rw-r--r--   1 user29 user29 655255 十一  2 02:25 twmask.csv
      -rw-rw-r--   1 user29 user29 380811 十一  2 23:06 twmasknew.csv
      ```

- 請問哪一間藥局擁有最多的兒童口罩 ? 藥局代號與名稱 ? 擁有多少兒童口罩 ? 地址與聯絡方式  ?

    - big
    
      >> child = FOREACH data GENERATE $0,$1,$2,$3,$5;
      
      >> child_desc = ORDER child BY $4 DESC;
      
      >> child_desc_1 = LIMIT child_desc 1;
      
      >> Dump child_desc_1;
      
      ```
      (5935011998,康安新美藥局,苗栗縣苗栗市中正路739號1樓,(037)373089,3820)
      ```

- 請將桃園縣也替換為桃園市, 並且再次統計全台灣各縣市的醫事機構

    - big
     
      >> data =load '/dataset/pig04/twmask.csv' USING PigStorage (',')AS(code: chararray,name:chararray,address:chararray,tel:chararray,smask:int,rmask:int,time:chararray);
      
      >> counties_replace = FOREACH counties GENERATE $0,REPLACE($1, '桃園縣','桃園市') ,$2;
      
      >> counties_gp = GROUP counties_replace BY $1;
      
      >> counties_count = FOREACH counties_gp GENERATE $0,COUNT($1);
      
      >> DUMP counties_count;

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

    - 使用 SUM 函數來統計每個群組(縣市)中的兒童口罩數量
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

    - 使用 SUM 函數來統計每個群組(區域)中的兒童口罩數量
    
      > sum_child = FOREACH gp_area GENERATE $0,SUM($1.$5);

    - 使用 ORDER 來排序兒童口罩數量
    
      > order_child = ORDER sum_child BY $1 DESC;

    - 使用 LIMIT 列出前三名
    
      > top3_child = LIMIT order_child 3;

* * *

## 第四週練習

- 臺北市地區的成人與兒童口罩的總和最多的前三個, 各幾個 ?

```
>> data2 = FOREACH data GENERATE $1, SUBSTRING($2,0,6), $4, $5;
total_mask = FOREACH data2 GENERATE $0, $1, $2+$3;
filter_mask = Filter total_mask BY $1 matches '臺北市.*';
group_mask = GROUP filter_mask BY $1;
sum_mask =  FOREACH group_mask GENERATE $0, SUM($1.$2);
order_total = ORDER sum_mask BY $1 DESC;
top3_child = LIMIT order_total 3;

```

>> dump top3_child;


- 找出臺北市的兒童口罩平均最多的前三個區域, 各幾個 ?

```

data2 = FOREACH data GENERATE $1, SUBSTRING($2,0,6), $5;
filter_mask = Filter total_mask BY $1 matches '臺北市.*';
group_mask = GROUP filter_mask BY $1;
avg_mask = FOREACH group_mask GENERATE $0, AVG($1.$2);
order_total = ORDER avg_mask BY $1 DESC;
top3_child = LIMIT order_total 3;

```

>> dump top3_child;


- 請找出全台灣人口最多的縣市

```
pop_data2 = FOREACH pop_data GENERATE SUBSTRING($0,0,3),$1;

gp_pop_data2 = GROUP pop_data2 BY$0;

counties_people = FOREACH gp_pop_data2 GENERATE $0, SUM($1.$1);

desc_counties = ORDER counties_people BY $1 DESC;

limit_counties = LIMIT desc_counties 1; 

```

>> dump limit_counties;

- 請找出臺北市各區域的口罩與人口的比例

```
join1 = JOIN counties_people BY $0, sum_adult_child BY $0; pop_mask = FOREACH join1 GENERATE $0 AS counties ,$1 AS people ,$3 AS mask;mask_per_people = FOREACH pop_mask GENERATE counties, (double)mask / (double)people; mask_per_people_round = FOREACH mask_per_people GENERATE $0, desc_mask_per = ORDER mask_per_people_round BY $1 DESC;
limit_desc = LIMIT desc_mask_per 3;
```

>> dump limit_desc;


- 請問口罩分配最公平的前三個縣市是 ?

```
join1 = JOIN counties_people BY $0, sum_adult_child BY $0; pop_mask = FOREACH join1 GENERATE $0 AS counties ,$1 AS people ,$3 AS mask;mask_per_people = FOREACH pop_mask GENERATE counties, (double)mask / (double)people; mask_per_people_round = FOREACH mask_per_people GENERATE $0, desc_mask_per = ORDER mask_per_people_round BY $1 DESC;
limit_desc = LIMIT desc_mask_per 3;

```

>> dump limit_desc;


* * *


