# ICT_Kafka 프로젝트 분석서

## 1. 프로젝트 개요

| 항목 | 내용 |
|------|------|
| 프로젝트명 | ICT_Kafka (이브와 ICT 멘토링 - Kafka) |
| 버전 | 1.0.0 |
| 언어 | Python |
| 목적 | 레거시 MySQL DB에서 데이터를 추출하여 Apache Kafka 토픽으로 실시간 전송하는 ETL 시스템 |

### 핵심 기능
- MySQL 데이터베이스에서 FLAG 기반 상태 관리를 통한 데이터 추출
- Kafka Exactly-Once Semantics(EOS) 트랜잭션을 이용한 메시지 전송
- FastAPI 기반 모니터링 REST API 서버
- 실시간(realtime) / 배치(batch) 두 가지 처리 모드 지원

---

## 2. 프로젝트 구조

```
ICT_Kafka/
├── README.md
├── 2022 이브와 Kafka.pdf
│
├── KafkaAdapter/                          # 메인 데이터 처리 모듈
│   ├── main.py                            # 진입점
│   ├── version.py                         # 버전 정보 (__version__ = "1.0.0")
│   ├── InterfaceProcess.py                # 핵심 비즈니스 로직
│   ├── config/
│   │   ├── config.xml                     # 메인 설정 파일 (DB, Kafka, 인터페이스)
│   │   ├── customer.sql                   # 데이터 추출 쿼리
│   │   ├── customer.sql.pre               # 전처리 SQL (FLAG: N→P)
│   │   └── customer.sql.post              # 후처리 SQL (FLAG: P→Y)
│   └── common/
│       ├── ConfigManager.py               # XML 설정 파서
│       ├── ConfigObject.py                # 설정 데이터 클래스
│       ├── CommonLogger.py                # 로깅 프레임워크
│       ├── CommonUtil.py                  # 유틸리티 함수
│       ├── KafkaWrapper.py                # Kafka Producer 래퍼
│       ├── MySQLWrapper.py                # MySQL 연결 래퍼
│       └── RestClient.py                  # REST API 클라이언트
│
└── KafkaAdapterMonitorServer/             # 모니터링 서버 모듈
    ├── main.py                            # FastAPI 서버 진입점
    ├── version.py                         # 버전 정보
    ├── config/
    │   └── config.xml                     # 서버 설정 파일
    └── common/
        ├── ConfigManager.py
        ├── ConfigObject.py
        ├── CommonLogger.py
        ├── CommonUtil.py
        ├── MySQLWrapper.py
        └── RestClient.py
```

---

## 3. 아키텍처

```
┌─────────────────────────────────────────────────┐
│          AWS RDS (ap-northeast-2, Seoul)         │
│                                                   │
│  legacydb.c0mdscslnpli.ap-northeast-2.rds...    │
│  ├── testdb    (CUSTOMER 테이블)                  │
│  └── monitordb (MONITOR_INFO 테이블)              │
└──────────┬────────────────────┬──────────────────┘
           │ MySQL              │ MySQL
           │                    │
┌──────────▼──────────┐  ┌─────▼──────────────────┐
│   KafkaAdapter      │  │ KafkaAdapterMonitor    │
│                     │  │ Server                  │
│  - DB 폴링 (100ms)  │  │                        │
│  - FLAG 상태 관리    │  │  FastAPI (0.0.0.0:8000)│
│  - Kafka 트랜잭션   │  │  POST /monitor_info/   │
│  - JSON 직렬화      │  │                        │
└──────────┬──────────┘  └────────────────────────┘
           │
           │ Kafka Protocol
           │
┌──────────▼──────────────────────┐
│       Kafka Cluster             │
│  52.79.77.151:9092              │
│  52.79.77.151:9093              │
│  52.79.77.151:9094              │
│                                  │
│  Topic: test_topic              │
└─────────────────────────────────┘
```

---

## 4. KafkaAdapter 상세 분석

### 4.1 main.py - 진입점

애플리케이션 부트스트랩을 담당한다. `CommonUtil`과 `InterfaceProcess`를 초기화하고 `data_get()`을 호출한다.

```python
common_util = CommonUtil.CommonUtil()
interface_process = InterfaceProcess.InterfaceProcess()
interface_process.data_get()
```

예외 발생 시 종료 코드 `-1`로 프로세스를 종료한다.

### 4.2 InterfaceProcess.py - 핵심 비즈니스 로직

#### 초기화
- 환경 변수 `KAFKA_ADAPTER_HOME`(기본: `.`)과 `KAFKA_ADAPTER_CONFIG`(기본: `./config/config.xml`)를 읽는다.
- `ConfigManager`, `MySQLWrapper`, `KafkaWrapper`, `RestClient`, `CommonUtil` 인스턴스를 생성한다.
- XML 설정 파일을 파싱하여 인터페이스 목록을 로드한다.

#### data_get() - 메인 처리 플로우

```
인터페이스 목록 순회 (type == "DBGET"):
│
├── 1. Kafka 연결 + 트랜잭션 초기화
├── 2. MySQL 연결
├── 3. SQL 파일 로드 (.pre / main / .post)
│
└── 4. 폴링 루프 (while True):
    │
    ├── (a) pre_sql 실행: FLAG='N' → 'P' (처리 대상 마킹)
    ├── (b) main sql 실행: FLAG='P'인 레코드 조회
    │
    ├── [데이터 존재 시]
    │   ├── Kafka 트랜잭션 시작 (begin_transaction)
    │   ├── 결과를 JSON 직렬화
    │   ├── Kafka 토픽에 메시지 전송 (kafka_put)
    │   ├── post_sql 실행: FLAG='P' → 'Y' (처리 완료)
    │   ├── Kafka 커밋 (commit_transaction)
    │   └── MySQL 커밋
    │
    ├── [데이터 없음 시]
    │   ├── MySQL 커밋
    │   └── realtime 모드: poll_time(100ms) 대기
    │
    └── batch 모드: 1회 실행 후 종료
        realtime 모드: 무한 반복
```

#### FLAG 상태 머신

```
  N (신규)  ──pre_sql──▶  P (처리중)  ──post_sql──▶  Y (완료)
                              │
                              │ (실패 시)
                              ▼
                         Rollback → 상태 유지
```

| FLAG | 의미 | 시점 |
|------|------|------|
| N | New - 미처리 레코드 | 초기 상태 |
| P | Processing - 처리 중 | pre_sql 실행 후 |
| Y | Yes(완료) - 처리 완료 | Kafka 전송 성공 후 |

#### 예외 처리
실패 시 Kafka 롤백(`abort_transaction`)과 MySQL 롤백이 모두 수행되어 데이터 정합성을 보장한다.

### 4.3 common 모듈

#### ConfigManager.py - XML 설정 파서

`config.xml` 파일을 `xml.etree.ElementTree`로 파싱하여 다음 설정을 제공한다:

| 메서드 | 반환 | 설명 |
|--------|------|------|
| `get_db_connection_info()` | dict | MySQL 접속 정보 |
| `get_kafka_connection_info()` | dict | Kafka Producer 설정 |
| `get_kafka_commit_timeout()` | int (초) | 커밋 타임아웃 |
| `get_kafka_transaction_timeout()` | int (초) | 트랜잭션 타임아웃 |
| `get_interface_info()` | list | 인터페이스 정의 목록 |
| `get_log_info()` | ConfigLogger | 로깅 설정 |
| `get_server_info()` | ConfigServer | 서버 바인딩 정보 |

- `transactional.id`에 `random_string.generate()`로 생성한 랜덤 접미사를 추가하여 인스턴스 간 고유성을 보장한다.
- 타임아웃 값은 밀리초(ms)에서 초(sec)로 변환된다.

#### KafkaWrapper.py - Kafka Producer 래퍼

Confluent Kafka Producer를 추상화한 클래스. 트랜잭션 기반 Exactly-Once Semantics(EOS)를 지원한다.

| 메서드 | 설명 |
|--------|------|
| `kafka_connect(info, auto_commit=False)` | Producer 인스턴스 생성. auto_commit=True 시 트랜잭션 비활성화 |
| `kafka_init_transaction()` | 트랜잭션 초기화 (최초 1회 호출 필수) |
| `kafka_begin_transaction()` | 트랜잭션 시작, 카운터 초기화 |
| `kafka_put(topic, message)` | 메시지를 UTF-8 인코딩 후 토픽에 전송 |
| `kafka_commit()` | 트랜잭션 커밋. 타임아웃 시 자동 재시도 로직 포함 |
| `kafka_rollback()` | 트랜잭션 중단 (abort) |
| `kafka_flush()` | 비트랜잭션 모드에서 메시지 플러시 |
| `kafka_disconnect()` | Producer 인스턴스 삭제 |

커밋 에러 처리 흐름:
```
KafkaException 발생
├── TIMED_OUT + retriable → 자동 재시도
├── retriable (기타) → 재시도
├── txn_requires_abort → 자동 롤백
└── 그 외 → 예외 전파
```

#### MySQLWrapper.py - MySQL 연결 래퍼

PyMySQL을 추상화한 클래스. DictCursor를 사용하여 컬럼명 기반 결과를 반환한다.

| 메서드 | 설명 |
|--------|------|
| `db_connect(info, auto_commit=False)` | MySQL 연결 |
| `db_select(fetchtype, sql)` | SELECT 실행. "all": fetchall(), "one": fetchone() |
| `db_insert(datafrm, table, option)` | pandas DataFrame 기반 INSERT |
| `db_execute(sql)` | DML/DDL 실행 (UPDATE, DELETE 등) |
| `db_commit()` | 트랜잭션 커밋 |
| `db_rollback()` | 트랜잭션 롤백 |
| `db_close()` | 연결 종료 |

#### CommonLogger.py - 로깅 프레임워크

Python `logging` 모듈을 래핑한 클래스. 주요 특징:

- **타임존**: Asia/Seoul (UTC → KST 변환)
- **로테이션**: `RotatingFileHandler` 사용 (설정 가능한 파일 크기/개수)
- **로그 포맷**: `[2024-01-01 12:00:00][PID:1234][INFO] message`
- **Hex Dump**: `data_dump=1` 설정 시 바이너리 데이터를 xxd 스타일로 출력
- **로그 레벨**: debug, info, warn, error, critical, exception

#### CommonUtil.py - 유틸리티

| 메서드 | 설명 |
|--------|------|
| `current_datetime()` | 현재 시각을 'YYYY-MM-DD HH:MM:SS' 형식으로 반환 |
| `make_json_result(...)` | 표준 JSON 응답 생성 (success, resultCode, resultMessage, data, pageInfo) |
| `make_page_data(result, page, size)` | 결과 목록 페이지네이션 |

#### ConfigObject.py - 설정 데이터 클래스

| 클래스 | 속성 |
|--------|------|
| `ConfigLogger` | path, max_size, file_count, level, data_dump, console_log |
| `ConfigServer` | ip, port |
| `ConfigInterface` | intf_type, intf_id, intf_in, intf_out, process_type, poll_time, column |
| `ConfigInterfaceColumn` | name, rename, replace, type, default, fk |

#### RestClient.py - REST 클라이언트

| 메서드 | 설명 |
|--------|------|
| `restapi_post_normal(url, body)` | JSON POST 요청 (Content-Type: application/json, UTF-8) |
| `restapi_get_normal(url)` | GET 요청 (리다이렉트 허용) |

---

## 5. KafkaAdapterMonitorServer 상세 분석

### 5.1 main.py - FastAPI 서버

모니터링 정보를 수신하여 DB에 저장하는 REST API 서버.

#### 엔드포인트

**POST /monitor_info/**

요청 모델 (`MonitorInfo` - Pydantic BaseModel):

| 필드 | 타입 | 설명 |
|------|------|------|
| id | str | 고유 식별자 |
| intf_id | str | 인터페이스 ID |
| intf_name | str | 인터페이스 이름 |
| host_id | str | 호스트 ID |
| process_dt | str | 처리 일시 |
| status | str | 상태 |
| error_message | str | 에러 메시지 |

처리 흐름:
1. 요청 데이터를 dict로 변환
2. `pandas.json_normalize()`로 DataFrame 생성
3. `MONITOR_INFO` 테이블에 INSERT
4. 커밋 후 연결 종료
5. 성공/실패 JSON 응답 반환

#### 서버 설정
- 바인딩: `0.0.0.0:8000`
- 프레임워크: FastAPI + Uvicorn
- 핫 리로드: 활성화 (`reload=True`)

### 5.2 common 모듈

KafkaAdapter와 동일한 구조의 공통 모듈을 사용한다 (ConfigManager, ConfigObject, CommonLogger, CommonUtil, MySQLWrapper, RestClient). 단, KafkaWrapper는 포함되지 않는다.

---

## 6. 설정 파일 분석

### 6.1 KafkaAdapter config.xml

#### 로깅 설정
```xml
<logger path="./kafka_adapter.log" file_size="10485760" count="10"
        level="info" data_dump="0" console_log="1"/>
```
- 로그 파일: `kafka_adapter.log`
- 파일 크기: 10MB, 백업 10개
- 콘솔 출력: 활성화

#### MySQL 설정
```xml
<db host="legacydb.c0mdscslnpli.ap-northeast-2.rds.amazonaws.com"
    user="admin" password="ictmentoring1!"
    database="testdb" charset="utf8mb4"
    cursorclass="pymysql.cursors.DictCursor"/>
```

#### Kafka Producer 설정
```xml
<kafka bootstrap.servers="52.79.77.151:9092,52.79.77.151:9093,52.79.77.151:9094"
       transactional.id="TID"
       transaction.timeout.ms="60000"
       acks="all"
       enable.idempotence="true"
       batch.size="1000000"
       batch.num.messages="10000"
       .../>
```

주요 설정 값:

| 설정 | 값 | 설명 |
|------|------|------|
| bootstrap.servers | 52.79.77.151:9092-9094 | 3개 브로커 |
| transactional.id | TID + 랜덤 접미사 | 트랜잭션 고유 ID |
| acks | all | 모든 복제본 확인 |
| enable.idempotence | true | 멱등성 활성화 |
| batch.size | 1,000,000 bytes | 배치 크기 |
| batch.num.messages | 10,000 | 배치당 최대 메시지 수 |
| queue.buffering.max.messages | 10,000,000 | 로컬 큐 최대 메시지 |
| queue.buffering.max.kbytes | 1,048,576 KB (1GB) | 로컬 큐 최대 크기 |
| message.send.max.retries | 2,147,483,647 | 재시도 횟수 (사실상 무제한) |
| partitioner | consistent_random | 파티셔너 전략 |
| compression | none | 압축 비활성화 |

#### 인터페이스 설정
```xml
<interface type="DBGET" intf_id="CUSTOMER"
           in="customer.sql" out="test_topic"
           process_type="realtime" poll_time="100"/>
```

| 속성 | 값 | 설명 |
|------|------|------|
| type | DBGET | DB 추출 타입 |
| intf_id | CUSTOMER | 인터페이스 식별자 |
| in | customer.sql | 입력 SQL 파일명 |
| out | test_topic | 출력 Kafka 토픽명 |
| process_type | realtime | 실시간 처리 (무한 루프) |
| poll_time | 100 | 폴링 간격 (ms) |

### 6.2 SQL 파일

**customer.sql.pre** (전처리):
```sql
UPDATE CUSTOMER SET FLAG = 'P' WHERE FLAG = 'N'
```

**customer.sql** (메인 조회):
```sql
SELECT ID, NAME, EMAIL FROM CUSTOMER WHERE FLAG = 'P'
```

**customer.sql.post** (후처리):
```sql
UPDATE CUSTOMER SET FLAG = 'Y' WHERE FLAG = 'P'
```

### 6.3 KafkaAdapterMonitorServer config.xml

```xml
<logger path="./server.log" file_size="10485760" count="10"
        level="info" data_dump="0" console_log="1"/>
<server ip="0.0.0.0" port="8000"/>
<db host="legacydb..." database="monitordb" .../>
```

---

## 7. 의존성

### Python 패키지

| 패키지 | 용도 | 사용 모듈 |
|--------|------|-----------|
| `confluent_kafka` | Kafka Producer/Consumer | KafkaAdapter |
| `pymysql` | MySQL 연결 | 공통 |
| `pandas` | DataFrame 기반 데이터 처리 | 공통 |
| `requests` | HTTP REST 클라이언트 | KafkaAdapter |
| `pytz` | 타임존 변환 (Asia/Seoul) | 공통 |
| `random_string` | 트랜잭션 ID 랜덤 접미사 생성 | KafkaAdapter |
| `fastapi` | REST API 프레임워크 | MonitorServer |
| `uvicorn` | ASGI 서버 | MonitorServer |
| `pydantic` | 요청 데이터 검증 | MonitorServer |

### 외부 서비스

| 서비스 | 엔드포인트 | 용도 |
|--------|-----------|------|
| AWS RDS MySQL | legacydb.c0mdscslnpli.ap-northeast-2.rds.amazonaws.com | 소스 DB (testdb) + 모니터 DB (monitordb) |
| Kafka Cluster | 52.79.77.151:9092-9094 | 메시지 브로커 (3개 브로커) |

---

## 8. 환경 변수

| 변수명 | 기본값 | 설명 | 사용 모듈 |
|--------|--------|------|-----------|
| `KAFKA_ADAPTER_HOME` | `.` | 어댑터 홈 디렉토리 | KafkaAdapter |
| `KAFKA_ADAPTER_CONFIG` | `./config/config.xml` | 설정 파일 경로 | KafkaAdapter |
| `SERVER_HOME` | (필수) | 서버 홈 디렉토리 | MonitorServer |
| `SERVER_CONFIG` | `./config/config.xml` | 서버 설정 파일 경로 | MonitorServer |

---

## 9. 트랜잭션 정합성 모델

이 시스템은 Kafka와 MySQL 양쪽에서 트랜잭션을 관리하여 데이터 정합성을 보장한다.

### 성공 경로
```
1. pre_sql  → MySQL: FLAG N→P
2. SELECT   → 데이터 조회
3. kafka_begin_transaction()
4. kafka_put() → 메시지 전송
5. post_sql → MySQL: FLAG P→Y
6. kafka_commit() → Kafka 트랜잭션 커밋
7. db_commit() → MySQL 트랜잭션 커밋
```

### 실패 경로
```
예외 발생 시:
1. kafka_rollback() → Kafka 트랜잭션 중단
2. db_rollback() → MySQL 롤백
→ FLAG 상태와 Kafka 메시지 모두 원복
```

### Exactly-Once Semantics (EOS)
- `transactional.id` 설정으로 트랜잭셔널 프로듀서 활성화
- `enable.idempotence=true`로 중복 메시지 방지
- `acks=all`로 모든 복제본의 확인 대기
- Consumer 측에서 `isolation.level=read_committed` 설정 시 완전한 EOS 보장
