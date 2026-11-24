import { Counter } from 'k6/metrics';
import { WebSocket } from 'k6/websockets';

// 부하 테스트 설정은 여기에서 변경한다.
const ratePerSecond = 20000;
const connectionCount = 20;
const durationSeconds = 10;
const drainSeconds = 5;
const symbolCount = 100;
const wsUrl = 'ws://localhost:8080/ws/admin/trades';

// 전체 목표 전송량을 연결 수로 나누어 연결 하나가 보낼 초당 메시지 수를 계산한다.
const messagesPerConnection = ratePerSecond / connectionCount;
// 연결별 목표 전송량을 일정한 간격으로 보내기 위한 간격(밀리초)이다.
const sendIntervalMs = 10;
const runId = String(Date.now());

// 설정 오류로 의도와 다른 부하가 발생하지 않도록 테스트 시작 전에 확인한다.
if (!Number.isInteger(ratePerSecond) || ratePerSecond < 1) {
	throw new Error('ratePerSecond must be a positive integer');
}
if (!Number.isInteger(connectionCount) || connectionCount < 1) {
	throw new Error('connectionCount must be a positive integer');
}
if (!Number.isInteger(durationSeconds) || durationSeconds < 1) {
	throw new Error('durationSeconds must be a positive integer');
}
if (!Number.isInteger(drainSeconds) || drainSeconds < 0) {
	throw new Error('drainSeconds must be a non-negative integer');
}
if (!Number.isInteger(symbolCount) || symbolCount < 1 || symbolCount > 10000) {
	throw new Error('symbolCount must be between 1 and 10000');
}
// 연결 하나에 초당 1,000건을 넘게 몰아 보내지 않도록 제한한다.
if (messagesPerConnection > 1000) {
	throw new Error('Use more connections so each connection sends no more than 1000 messages per second');
}

// VU(Virtual User) 하나가 WebSocket 연결 하나를 맡는다.
// 각 VU는 한 번 실행되어 소켓을 열고, 전송과 응답 대기를 수행한다.
export const options = {
	scenarios: {
		admin_trade_websocket_load: {
			executor: 'per-vu-iterations',
			vus: connectionCount,
			iterations: 1,
			// 전송 시간과 응답 대기 시간, 종료 여유 시간을 합한 최대 실행 시간이다.
			maxDuration: `${durationSeconds + drainSeconds + 10}s`,
		},
	},
};

// k6 결과에 표시할 누적 개수 지표다.
const messagesSent = new Counter('ws_messages_sent');
const messagesAccepted = new Counter('ws_messages_accepted');
const messagesRejected = new Counter('ws_messages_rejected');
const sendErrors = new Counter('ws_send_errors');
const connectionErrors = new Counter('ws_connection_errors');

export default function () {
	// 이 VU가 테스트 서버에 WebSocket 연결을 연다.
	const socket = new WebSocket(wsUrl);
	// 메시지마다 종목과 가격을 바꾸기 위한 연결별 순번이다.
	let sequence = 0;
	const pending = new Set();

	socket.addEventListener('open', () => {
		// VU에 따라 한국 시장과 미국 시장을 번갈아 지정한다.
		const market = __VU % 2 === 1 ? 'KR' : 'US';
		const startedAt = Date.now();
		const durationMs = durationSeconds * 1000;
		const sendDueTrades = () => {
			const elapsed = Math.min(Date.now() - startedAt, durationMs);
			const target = Math.floor(elapsed * messagesPerConnection / 1000);
			while (sequence < target && socket.readyState === 1) {
				const symbolNumber = String(sequence % symbolCount).padStart(4, '0');
				const tradeId = `${runId}-${__VU}-${sequence}`;
				const trade = {
					tradeId,
					market,
					symbol: `LOAD${symbolNumber}`,
					price: (10000 + sequence % 10000) / 100,
				};
				sequence += 1;
				try {
					socket.send(JSON.stringify(trade));
					pending.add(tradeId);
					messagesSent.add(1);
				} catch (error) {
					sendErrors.add(1);
				}
			}
		};
		const sendInterval = setInterval(sendDueTrades, sendIntervalMs);

		setTimeout(() => {
			clearInterval(sendInterval);
			sendDueTrades();
			setTimeout(() => socket.close(), drainSeconds * 1000);
		}, durationMs);
	});

	socket.addEventListener('message', (event) => {
		try {
			// 서버가 돌려준 JSON 응답의 상태에 따라 수락과 거부를 나눈다.
			const response = JSON.parse(event.data);
			if (response.status === 'accepted' && pending.delete(response.tradeId)) {
				// 서버가 체결을 수신하고 검증한 직후 보낸 확인 응답을 센다.
				// 서버는 이 응답을 전송한 뒤 체결 처리 코드를 호출한다.
				messagesAccepted.add(1);
			} else {
				messagesRejected.add(1);
			}
		} catch (error) {
			// JSON이 아니거나 읽을 수 없는 응답은 거부 건수에 포함한다.
			messagesRejected.add(1);
		}
	});

	socket.addEventListener('error', () => {
		// WebSocket 연결 중 발생한 오류 횟수다.
		connectionErrors.add(1);
	});
}
