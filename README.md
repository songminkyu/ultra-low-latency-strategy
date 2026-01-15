# 초 저지연 전략

이 저장소는 초(低) 저지연(ultra-low-latency) 스트리밍 처리와 메시징 전략을 실험하고 정리한 예제 모음입니다. 루트에는 주요 실험/예제가 두 개의 디렉토리에 정리되어 있습니다.

## 디렉토리 구조

- Flink_Real_time/
  - Apache Flink를 사용한 실시간 처리 예제 및 실험 코드와 설정이 들어있는 디렉토리입니다. 낮은 지연을 위한 파이프라인 설계 및 최적화 관련 실험을 포함합니다.
- Nats_Jetstream/
  - NATS JetStream 기반의 메시징 및 스트림 저장/처리 관련 예제가 포함되어 있습니다. 메시지 전송 지연 최소화와 내구성(내결함성)을 함께 고려한 구성들이 포함됩니다.

## 빠른 시작

1. 저장소를 클론합니다.

   git clone https://github.com/songminkyu/ultra-low-latency-strategy.git
   cd ultra-low-latency-strategy

2. 각 예제 디렉토리로 이동하여 README 또는 예제별 문서를 확인하세요.
   - Flink_Real_time/ 안의 README 또는 스크립트를 참고해 Flink 클러스터(또는 로컬 포함) 설정 및 실행방법을 따릅니다.
   - Nats_Jetstream/ 안의 설정 문서를 참고해 NATS 서버와 JetStream 구성, 예제 실행 방법을 따릅니다.

3. 일반 요구사항
   - Java 11+ (또는 예제에 명시된 버전)
   - Apache Flink (예제에 따라 로컬 또는 클러스터 모드)
   - NATS Server (JetStream 사용 시 JetStream 활성화)
   - Docker / Docker Compose (예제에 따라 권장)

## 목표

- 메시지 입·출력 및 스트림 처리 파이프라인의 엔드투엔드 지연을 최소화하는 기법을 정리합니다.
- 실제 서비스 수준의 성능을 목표로 구성/설정/코드 샘플을 제공합니다.
- Flink와 NATS(JetStream)를 중심으로한 아키텍처 패턴을 비교·검토합니다.

## 기여

구현 개선, 실험 결과, 또는 문서 개선 기여를 환영합니다. Pull request 또는 issue로 제안해주세요.

## 라이선스

특별히 명시되지 않은 경우, 이 저장소의 코드는 기본적으로 공개 샘플 용도로 제공됩니다. 라이선스를 추가하고 싶으시면 PR로 제안해 주세요.

---

(루트에 있는 각 디렉토리의 상세 내용은 각 디렉토리 안의 README나 문서를 참고하세요.)