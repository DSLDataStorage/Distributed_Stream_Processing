# Distributed Stream Processing
분산 환경에서 스트리밍 데이터와 저장된 데이터를 실시간으로 통합해서 처리 및 분석


![image](https://user-images.githubusercontent.com/50433145/173730219-a98b8482-3bb3-4bff-bf76-a015b8f4a2bb.png)<br>
그림 1. 분산 환경에서 스트리밍 데이터와 저장된 데이터의 실시간 통합 처리 및 분석

IoT의 확산에 따라 그림 1과 같이 끊임없이 들어오는 스트리밍 빅데이터와 저장된 빅데이터의 통합 처리 및 분석에 대한 산업계의 수요가 증가하고 있으므로 분산 환경에서 이를 효율적으로 지원하는 방법에 대한 연구가 필요함

## 연구 목표
* 스트리밍 데이터와 저장된 데이터의 통합을 위한 실시간 분산 동등조인 연구
  * 빅데이터의 volume 및 velocity 요구사항을 충족시키기 위해 분산 환경에서 정형 데이터에 적용 가능한 실시간 동등조인(equi-join) 방법 개발
* 스트리밍 데이터와 저장된 데이터 간의 실시간 분산 유사조인 및 데이터 융합 연구
  * 빅데이터의 variety 요구사항을 충족시키기 위해 분산 환경에서 비정형 데이터에 적용 가능한 실시간 유사조인(similarity join) 방법 개발
  * 빅데이터의 veracity 요구사항을 충족시키기 위해 데이터 융합(data fusion) 기술 연구
* 스트리밍 데이터와 저장된 데이터를 통합한 실시간 분산 머신러닝 연구
  * 빅데이터의 value 요구사항을 충족시키기 위해 분산 환경에서 스트리밍 데이터와 저장된 데이터를 통합하여 실시간 분석이 가능한 온라인 머신러닝 방법 개발


## Results
* [DS-Join](https://github.com/DSLDataStorage/Distributed_Stream_Processing/tree/master/DS-Join)
* [DSim-Join](https://github.com/DSLDataStorage/Distributed_Stream_Processing/tree/master/DSim-Join)
* [S3M](https://github.com/DSLDataStorage/Distributed_Stream_Processing/tree/master/S3M)
