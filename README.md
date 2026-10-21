# defomo
실시간 체결 데이터를 기반으로 목표 가격 도달을 알려주는 주가 알림 서비스

## 토스증권 실시간 체결 수신

Spring Boot 3.5 기반으로 토스증권 Open API에 인증하고, 웹소켓에서 국내·미국 주식 체결 데이터를 받습니다. 기본 설정에서는 외부 API 연결을 비활성화합니다.

### 실행 설정

JDK 21 이상을 준비합니다. Gradle은 저장소에 포함된 Wrapper를 사용합니다. 저장소 루트의 `.env.example`을 `.env`로 복사한 뒤 토스증권에서 발급받은 `TOSSINVEST_CLIENT_ID`, `TOSSINVEST_CLIENT_SECRET`을 입력합니다. 두 값이 모두 설정되어 있으면 앱 실행 시 웹소켓 연결을 자동으로 시도합니다. 구독할 종목 코드는 쉼표로 구분합니다.

`.env`는 Git에서 제외되며, Spring Boot가 실행 시 자동으로 읽습니다. 실제 인증 정보를 `.env.example`이나 다른 추적 파일에 넣지 마세요.

```bash
cp .env.example .env
# .env 파일에서 인증 정보, 구독 종목, 필요 시 DB 연결 정보를 편집
docker compose up -d
./gradlew bootRun
```

Docker Compose는 Kafka, Kafka UI, MySQL을 실행합니다. Kafka UI는 `http://localhost:8081`에서 확인할 수 있습니다. 앱은 Kafka 체결 이벤트를 짧은 구간으로 집계해 발행하고, MySQL에 저장된 활성 알림 조건과 비교합니다. 조건을 충족하면 알림 상태를 `FIRED`로 변경합니다. 실제 알림 발송은 아직 구현하지 않았습니다.

체결 수신을 시작하면 연결, 구독 결과와 종목별 첫 체결 요약이 INFO 로그에 기록됩니다. 이후 개별 체결 요약은 DEBUG 로그에 기록됩니다. 거래가 없는 종목은 구독 성공 상태여도 체결 로그가 나타나지 않을 수 있습니다.

웹소켓 연결 전에 토스증권 Open API 설정에서 실행 환경의 공인 IP가 허용되어 있는지 확인해야 합니다.

한 시장에서 최대 100개 종목까지 설정할 수 있습니다. 국내와 미국 시장은 각각 별도 웹소켓 연결을 사용합니다. 연결 종료 후 자동 재연결은 아직 구현하지 않았습니다.

### 관련 문서

- [토스증권 개발자 문서](https://developers.tossinvest.com/docs)
- [토스증권 웹소켓 명세](https://openapi.tossinvest.com/openapi-docs/latest/asyncapi.json)
