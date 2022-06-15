# 세미-스트림 서브시퀀스 매칭을 이용한 온라인 머신러닝
![figure-Page-1 (1)](https://user-images.githubusercontent.com/50433145/173738812-6f2b262e-1cac-4ed1-9ef8-ae74f21236e6.png)<br>
그림 1. 세미-스트림 서브시퀀스 매칭을 이용한 온라인 머신러닝 방법

* 서브시퀀스 매칭을 이용하여 저장된 데이터에서 스트리밍 데이터와 가장 유사한 패턴을 갖는 데이터 시퀀스 검색
* 세미-스트림 연산을 이용하여 스트리밍 데이터와 저장된 데이터를 결합
* 결합한 데이터를 이용하여 온라인 머신러닝 모델을 학습시키고, 실시간으로 미래에 들어올 데이터의 값을 예측
* 실시간으로 들어오는 데이터에는 종속변수 값이 존재하지 않으므로 큐를 이용하여 지연된 학습

## 실험결과
![ml_exp_2](https://user-images.githubusercontent.com/50433145/173739180-520a34ab-209c-4a11-84a5-f1699dd1a036.png)<br>
그림 2. 윈도우 간격에 따른 예측 모델의 MSE 변화
* 실제 시계열 데이터를 이용하여 실험한 결과(그림 2) 제안한 방법(S3M)이 스트리밍 데이터만 사용한 방법(MLStream)에 비해 MSE를 평균 6.61% 감소시킴

## Paper
이종학, 김홍지 and 이기훈, "[세미-스트림 서브시퀀스 매칭을 이용한 온라인 머신러닝](https://www.kci.go.kr/kciportal/ci/sereArticleSearch/ciSereArtiView.kci?sereArticleSearchBean.artiId=ART002618762)," 데이타베이스연구, Vol.36, No.2, pp.17-27, Aug 2020. (ISSN: 1598-9798, in Korean)
