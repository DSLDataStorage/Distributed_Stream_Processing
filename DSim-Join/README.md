# Semi-Stream Similarity Join Processing in a Distributed Environment
![Architecture](https://user-images.githubusercontent.com/50433145/173737283-d34c05ea-a94d-4907-8d0b-4ab2e5b1fc2e.png)
그림 1. 세미-스트림 분산 유사조인 처리 구조
* DS-Join의 동등조인 기술을 유사조인으로 확장하여 적용
* 분산 스트림 처리 엔진(Stream Processing Engine; SPE) 내에 분산 캐시를 구현하여 원격 DB 서버와의 통신을 최소화
* 데이터 파티셔닝 제어를 통한 네트워크 통신 최소화
* 유사조인의 모든 처리 단계를 완전 분산 병렬화
* 유사조인의 특성을 고려하여 분산 캐시의 크기를 동적으로 최적화


## How to compile spark project
```
> sbt clean assembly
```
## How to run code
```
example)
> ./assembly_run.sh 1000 2 1 DS_Sim -> run DS_Sim class, and run mesos cluster, using musical_1000(mongodb)  
> ./assembly_run.sh 3000 0 1 DS_join -> run DS_join class, and run local mode, using musical_3000(mongodb)
```
argument 1 : data number.<br>
argument 2 : isDistributed? (0: local, 1: standalone, 2:cluster(mesos)).<br>
argument 3 : sbt clean or not? (0: not compile, 1: compile).<br>
argument 4 : class name.


## Paper
Hong-Ji Kim and Ki-Hoon Lee, “[Semi-Stream Similarity Join Processing in a Distributed Environment](https://ieeexplore.ieee.org/document/9141233),” IEEE Access, Vol. 8, pp. 130194-130204 , July 2020. (ISSN: 2169-3536)
