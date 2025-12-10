import collections
import random
import time

# --- 환경 설정 ---
TOTAL_NODES = 4
# 비잔틴 장애 허용 한계: t = (n-1) // 3. n=4일 때, t=1. (1개의 악의적 노드 허용)
FAULTY_LIMIT = (TOTAL_NODES - 1) // 3 
NODES = []

# --- 노드 클래스 정의 (복제본, Replica) ---
class PBFTNode:
    def __init__(self, node_id, total_nodes):
        self.id = node_id
        self.total_nodes = total_nodes
        self.is_primary = (node_id == 0) # 초기 Primary 노드는 0번
        self.is_faulty = False
        self.state = {'last_seq': 0} # 현재 상태
        # {seq_num: {msg_type: set_of_sender_ids}}
        self.log = collections.defaultdict(lambda: collections.defaultdict(set)) 
        
        print(f"Node {self.id} initialized. Fault limit (t): {FAULTY_LIMIT}")

    def set_faulty(self, status):
        """노드를 악의적으로 설정"""
        self.is_faulty = status
        print(f"[N{self.id}] ⚠️ Node set to MALICIOUS.")

    def receive_request(self, request, sender_id):
        """클라이언트 요청 처리 시작"""
        if self.is_faulty and random.random() < 0.5:
            # 악의적인 Primary 노드는 요청을 무시하거나 지연시킬 수 있음
            print(f"[N{self.id}] 😈 Malicious Primary ignoring client request...")
            return

        if self.is_primary:
            # 1. Pre-Prepare 단계 시작
            self.state['last_seq'] += 1
            seq_num = self.state['last_seq']
            
            print(f"\n--- [P{self.id}] Starting Round {seq_num}: Request '{request}' ---")
            
            # Primary가 악의적일 경우, 거짓 메시지를 보낼 수 있음
            if self.is_faulty:
                 malicious_request = "Transfer $100 to Bob" # Alice 대신 Bob에게 전송하도록 변조
                 print(f"[P{self.id}] 😈 Sending malicious PRE-PREPARE: '{malicious_request}'")
                 self.broadcast_message('PRE-PREPARE', seq_num, malicious_request)
            else:
                 self.broadcast_message('PRE-PREPARE', seq_num, request)

    def receive_message(self, msg_type, seq_num, request, sender_id):
        """노드 간 메시지 수신 및 처리"""
        
        # 악의적인 노드는 Prepare/Commit 메시지를 가끔 무시하거나 변경한다고 가정
        if self.is_faulty and msg_type in ['PREPARE', 'COMMIT'] and random.random() < 0.3:
            # Prepare/Commit 메시지 수집을 방해
            print(f"[N{self.id}] 😈 Maliciously ignoring or altering {msg_type} from N{sender_id}")
            return
        
        # 메시지 로그 업데이트
        self.log[seq_num][msg_type].add(sender_id)
        
        # 모든 노드는 Primary로부터의 Pre-Prepare 메시지를 기반으로 Prepare 시작
        if msg_type == 'PRE-PREPARE' and sender_id == 0: # Primary 노드가 0번이라고 가정
            # 2. Prepare 단계 시작
            print(f"[N{self.id}] Rcvd PRE-PREPARE for seq {seq_num}. Starting Prepare.")
            self.broadcast_message('PREPARE', seq_num, request)

        elif msg_type == 'PREPARE':
            # 3. Commit 단계 시작 조건 확인
            prepare_count = len(self.log[seq_num]['PREPARE'])
            # 2t 이상의 Prepare 메시지를 받으면 '준비됨(Prepared)'
            if prepare_count >= 2 * FAULTY_LIMIT and 'COMMIT' not in self.log[seq_num]:
                print(f"[N{self.id}] Prepared for seq {seq_num} (count: {prepare_count}). Starting Commit.")
                self.broadcast_message('COMMIT', seq_num, request)

        elif msg_type == 'COMMIT':
            # 4. 확정 단계 조건 확인
            commit_count = len(self.log[seq_num]['COMMIT'])
            # 2t+1 이상의 Commit 메시지를 받으면 '확정됨(Committed)'
            if commit_count >= 2 * FAULTY_LIMIT + 1:
                # 합의가 도출된 경우, 더 이상 메시지를 보내지 않도록 방지
                if 'EXECUTED' not in self.log[seq_num]:
                    self.log[seq_num]['EXECUTED'] = set([self.id])
                    print(f"[N{self.id}] ✅ Committed for seq {seq_num} (count: {commit_count}). Executing request: '{request}'")
                    # 5. 응답 (Reply) 단계 시뮬레이션 (여기서는 간단히 출력)
                
    def broadcast_message(self, msg_type, seq_num, request):
        """네트워크 전체에 메시지 전파 (시뮬레이션)"""
        for node in NODES:
            if node.id != self.id:
                node.receive_message(msg_type, seq_num, request, self.id)

# --- 시뮬레이션 실행 ---

NODES = [PBFTNode(i, TOTAL_NODES) for i in range(TOTAL_NODES)]

# N1을 악의적인 노드로 설정 (t=1 조건 내)
NODES[1].set_faulty(True)

client_request = "Transfer $100 to Alice"

# 클라이언트 요청 시뮬레이션 (요청은 Primary 노드인 N0으로 직접 보냄)
NODES[0].receive_request(client_request, 'Client')

# 시뮬레이션 종료 대기
time.sleep(0.5)
