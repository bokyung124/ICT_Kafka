# ICT Kafka Adapter 프로젝트 - 이력서 & 면접 준비

---

## 이력서 활동 목록 (간결하게)

> **빅데이터 수집을 위한 Kafka Adapter 어플리케이션 개발** (ICT 멘토링)
> - Legacy DB(MySQL)의 데이터를 실시간으로 Apache Kafka로 수집하는 어댑터 개발
> - Kafka Exactly-Once Semantics(EOS) 기반 트랜잭션 처리, FLAG 상태머신 설계, FastAPI 모니터링 서버 구현
> - Python, Apache Kafka, MySQL, FastAPI, Docker

---

## 면접 예상 질문 & 답변 가이드

### Q1. "이 프로젝트가 뭔지 한마디로 설명해주세요"

Legacy 시스템의 MySQL DB에 쌓이는 데이터를 **실시간으로 Kafka 토픽에 전송**하는 어댑터입니다.
DB를 주기적으로 폴링하면서, 아직 처리되지 않은 데이터를 Kafka에 넣고, Kafka 트랜잭션과 DB 트랜잭션을 **동기화**해서 데이터 유실이나 중복 전송을 방지합니다.

---

### Q2. "왜 Kafka를 사용했나요?"

- 대용량 데이터를 **비동기적으로 안정적으로 전달**하기 위해
- Producer-Consumer 구조로 **시스템 간 결합도를 낮추기** 위해
- Kafka의 **Exactly-Once Semantics(EOS)**를 활용해 데이터 정합성을 보장하기 위해
- 추후 다수의 Consumer가 동일한 데이터를 **독립적으로 소비**할 수 있는 확장성

---

### Q3. "아키텍처를 설명해주세요"

```
[MySQL DB] → [KafkaAdapter] → [Kafka Cluster(3 brokers)] → [Consumer들]
                  ↓
          [MonitorServer(FastAPI)]
```

- **KafkaAdapter**: DB 폴링 → 데이터 추출 → Kafka 전송 (핵심)
- **Kafka Cluster**: 3개 브로커 구성으로 고가용성 확보
- **MonitorServer**: 전송 결과를 REST API로 수신하여 모니터링

---

### Q4. "데이터 정합성은 어떻게 보장했나요?" (핵심 질문)

**FLAG 기반 상태머신 + Kafka 트랜잭션 동기화**로 보장했습니다.

| 단계 | FLAG | 동작 |
|------|------|------|
| 1 | N → P | 처리 대상 데이터를 "Processing" 상태로 변경 |
| 2 | P | FLAG='P'인 데이터만 SELECT |
| 3 | - | Kafka 트랜잭션 시작 → 데이터 전송 |
| 4 | P → Y | Kafka commit 성공 후 FLAG를 "완료"로 변경 |

- Kafka 전송 실패 시: **Kafka rollback + DB rollback** → FLAG가 P로 유지되어 재처리 가능
- 이렇게 하면 **데이터 유실도, 중복 전송도 방지**됩니다

---

### Q5. "Exactly-Once Semantics(EOS)가 뭔가요?"

- 메시지가 **정확히 한 번만** 전달되는 것을 보장하는 Kafka의 기능
- `enable.idempotence=true` + `transactional.id` 설정으로 활성화
- Producer가 `begin_transaction()` → `produce()` → `commit_transaction()` 순서로 동작
- 네트워크 장애 등으로 재전송되더라도 Kafka가 **중복을 자동 제거**

---

### Q6. "설계할 때 어려웠던 점이나 고민한 점은?"

**1) DB 트랜잭션과 Kafka 트랜잭션의 동기화**
- 두 개의 서로 다른 시스템(DB, Kafka)의 트랜잭션을 완전히 atomic하게 만들 수 없음
- FLAG 상태머신을 도입해서, 실패 시 재처리가 가능한 구조로 설계

**2) 모니터링 실패가 메인 파이프라인을 중단시키면 안 됨**
- MonitorServer 전송 로직을 try-except로 격리
- 모니터링은 부가 기능이므로, 실패해도 데이터 전송 파이프라인은 정상 동작

**3) 설정의 유연성**
- XML 설정 파일로 DB/Kafka/인터페이스 정보를 외부화
- 코드 수정 없이 다양한 테이블/토픽 조합을 처리 가능하게 설계

---

### Q7. "모니터링 서버는 왜 만들었나요?"

- 실시간으로 **어떤 인터페이스가 성공/실패했는지** 추적하기 위해
- FastAPI로 REST API 서버를 구현하고, 각 전송 건마다 상태(SUCCESS/EMPTY/ERROR)를 기록
- 운영 환경에서 **장애 감지와 데이터 흐름 모니터링**에 활용

---

### Q8. "Docker는 어떻게 활용했나요?"

- `docker-compose.yml`로 **Kafka 3 브로커 + Zookeeper** 클러스터를 구성
- 로컬 개발 환경에서도 실제 운영 환경과 유사한 **멀티 브로커 환경**을 재현
- 인프라를 코드로 관리(IaC)하여 환경 재현이 용이

---

### Q9. "개선하고 싶은 점이 있다면?"

솔직하게 말하면 좋은 포인트들:
- **Consumer 측 구현 추가**: 현재는 Producer만 있으므로, Consumer + 데이터 적재까지
- **에러 재처리 로직 고도화**: FLAG='P' 상태로 남은 데이터의 타임아웃 처리
- **모니터링 대시보드**: 현재 API만 있고, 시각화 화면이 없음
- **테스트 코드 추가**: 단위 테스트/통합 테스트 부재

---

## 면접 팁

- **"왜"를 중심으로 말하기**: "Kafka를 썼다"가 아니라 "데이터 유실 방지를 위해 EOS를 지원하는 Kafka를 선택했다"
- **트레이드오프 언급하기**: "완벽한 2PC는 불가능하지만, FLAG 패턴으로 재처리 가능한 구조를 택했다"
- **개선점을 솔직히 말하기**: 부족한 점을 인지하고 있다는 것 자체가 강점

질문|대비 답변
|---|---|
"Consumer Group이 뭔지 아세요?"|같은 group.id를 가진 Consumer들이 토픽의 파티션을 나눠서 소비하는 구조
"offset은 뭔가요?"|Consumer가 어디까지 읽었는지 추적하는 위치값. auto commit 또는 manual commit으로 관리
"rebalancing은?"|Consumer가 추가/제거될 때 파티션 할당이 재조정되는 과정