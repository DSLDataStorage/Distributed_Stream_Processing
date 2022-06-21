# Distributed Join Processing Between Streaming and Stored Big Data Under the Micro-Batch Model
<img src=https://user-images.githubusercontent.com/50433145/173737741-6b628052-44e6-4933-84d1-a25db5839355.png width="80%" height="80%"><br>
그림 1. 스트리밍 데이터와 저장된 데이터의 분산 동등조인 처리 구조

* 최신 분산 스트림 처리 엔진(Stream Processing Engine; SPE)의 마이크로 배치 모델에 기반하여 고장 허용성을 제공함
* 데이터 파티셔닝을 제어하여 네트워크 통신을 최소화함
* 동등조인의 모든 처리 단계를 완전 분산 병렬화
* 분산 SPE 내에 분산 캐시를 구현하여 원격 DB 서버와의 통신을 최소화
* 분산 캐시의 크기를 동적으로 조정하여 분산 SPE의 로드와 DB 서버의 로드를 동적으로 밸런싱

## 실험결과
<img src=https://user-images.githubusercontent.com/50433145/173738337-81f0bdd4-aacf-4948-8147-5a3340612e2c.png width="50%" hegiht="50%"><br>
그림 2. 데이터베이스 크기 변화에 따른 TPC-H 조인 처리율
* TPC-H 벤치마크 기반 실험 결과(그림 2) 제안한 방법(DS-Join)이 기존 방법에 비해 최대 2.2배의 성능 향상을 보이며 데이터베이스의 크기가 커질수록 성능이 

## Paper
Young-Ho Jeon, Ki-Hoon Lee, and Ho-Jun Kim, “[Distributed Join Processing between Streaming and Stored Big Data under the Micro-Batch Model](https://ieeexplore.ieee.org/document/8666990),” IEEE Access, Vol. 7, pp. 34583-34598, Mar. 2019. (ISSN: 2169-3536)

