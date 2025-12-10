import random
from typing import List, Dict, Optional, Tuple

# === 1. PAXOS 구성 요소 정의 ===

class Proposal:
    """PAXOS에서 합의를 위한 제안(Transaction)을 나타내는 클래스"""
    def __init__(self, proposal_id: int, value: str):
        self.proposal_id = proposal_id  # 제안 번호 (더 높은 번호가 우선)
        self.value = value              # 제안 값 (트랜잭션 명령)
    
    def __repr__(self):
        return f"Proposal(id={self.proposal_id}, value='{self.value}')"

class Acceptor:
    """PAXOS의 Acceptor 역할을 시뮬레이션하는 클래스 (분산 스토리지 노드)"""
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.promised_id = -1          # 약속한 가장 높은 제안 번호 (Prepare Phase)
        self.accepted_proposal: Optional[Proposal] = None  # 수락한 제안
        self.current_balance: Dict[str, float] = {}       # 현재 계좌 잔액 상태

    def __repr__(self):
        return f"Acceptor(ID={self.node_id}, Balance={self.current_balance})"

    def prepare(self, proposer_id: int, proposal_id: int) -> Tuple[bool, Optional[Proposal]]:
        """Phase 1a: Prepare 요청 처리"""
        if proposal_id > self.promised_id:
            # 더 높은 번호의 제안에 대해 약속하고 응답
            self.promised_id = proposal_id
            print(f"  [A{self.node_id}] P{proposer_id}의 Prepare({proposal_id}) 수락. 약속: {proposal_id}")
            return True, self.accepted_proposal
        else:
            # 이미 더 높은 번호에 약속했으므로 거부
            print(f"  [A{self.node_id}] P{proposer_id}의 Prepare({proposal_id}) 거부. 이미 약속된 ID: {self.promised_id}")
            return False, None

    def accept(self, proposer_id: int, proposal: Proposal) -> bool:
        """Phase 2a: Accept 요청 처리"""
        if proposal.proposal_id >= self.promised_id:
            # 약속한 번호보다 크거나 같으면 수락
            self.promised_id = proposal.proposal_id
            self.accepted_proposal = proposal
            self._execute_transaction(proposal.value)
            print(f"  [A{self.node_id}] P{proposer_id}의 Accept({proposal.proposal_id}, '{proposal.value}') 수락.")
            return True
        else:
            # 약속된 번호보다 낮으므로 거부
            print(f"  [A{self{self.node_id}] P{proposer_id}의 Accept({proposal.proposal_id}, '{proposal.value}') 거부. 약속된 ID: {self.promised_id}")
            return False

    def _execute_transaction(self, command: str):
        """실제 은행 거래 로직 (분산 스토리지 상태 변경)"""
        parts = command.split()
        action = parts[0]
        account = parts[1]
        
        if action == "OPEN":
            if account not in self.current_balance:
                self.current_balance[account] = float(parts[2])
                print(f"    -> [A{self.node_id}] 거래 실행: {account} 계좌 개설 및 {parts[2]} 입금.")
        elif action == "DEPOSIT":
            amount = float(parts[2])
            if account in self.current_balance:
                self.current_balance[account] += amount
                print(f"    -> [A{self.node_id}] 거래 실행: {account}에 {amount} 입금.")
        elif action == "WITHDRAW":
            amount = float(parts[2])
            if account in self.current_balance and self.current_balance[account] >= amount:
                self.current_balance[account] -= amount
                print(f"    -> [A{self.node_id}] 거래 실행: {account}에서 {amount} 출금.")
            elif account in self.current_balance and self.current_balance[account] < amount:
                print(f"    -> [A{self.node_id}] 거래 실패: {account} 잔액 부족.")
                # 잔액 부족은 합의 알고리즘의 Safety 문제가 아니므로, 여기서는 단순히 로그만 남김
            else:
                print(f"    -> [A{self.node_id}] 거래 실패: {account} 계좌 없음.")

class Proposer:
    """PAXOS의 Proposer 역할을 시뮬레이션하는 클래스"""
    def __init__(self, proposer_id: int, acceptors: List[Acceptor]):
        self.proposer_id = proposer_id
        self.acceptors = acceptors
        self.next_proposal_id = self.proposer_id  # 각 제안자의 고유 ID로 시작

    def propose(self, transaction_command: str) -> bool:
        """PAXOS 합의 과정 실행 (2 Phase Commit)"""
        print(f"\n=== P{self.proposer_id}: '{transaction_command}' 거래 시작 ===")
        
        # 1. 제안 번호 생성 및 Prepare Phase
        current_id = self.next_proposal_id
        self.next_proposal_id += len(self.acceptors) # 다음 ID를 더 높게 설정
        
        print(f"  [P{self.proposer_id}] Phase 1: Prepare({current_id}) 요청.")

        promises = 0
        accepted_value: Optional[str] = transaction_command
        highest_accepted_id = -1

        # Acceptor들에게 Prepare 요청
        for acceptor in self.acceptors:
            if random.random() > 0.1: # 10% 확률로 노드 실패/응답 없음 가정
                is_promised, previously_accepted = acceptor.prepare(self.proposer_id, current_id)
                if is_promised:
                    promises += 1
                    if previously_accepted and previously_accepted.proposal_id > highest_accepted_id:
                        # 더 높은 번호로 이미 합의된 값이 있으면 그 값을 사용해야 함 (PAXOS Safety 보장)
                        highest_accepted_id = previously_accepted.proposal_id
                        accepted_value = previously_accepted.value
                
        quorum = len(self.acceptors) // 2 + 1
        
        if promises < quorum:
            print(f"  [P{self.proposer_id}] Prepare 실패. 응답 수: {promises}, 정족수: {quorum}. 재시도 필요.")
            return False

        # Phase 2: Accept Phase
        # 만약 이전에 더 높은 ID로 수락된 값이 있다면, 그 값을 제안값으로 사용 (Safety)
        proposal_value_to_use = accepted_value if accepted_value else transaction_command
        current_proposal = Proposal(current_id, proposal_value_to_use)
        
        print(f"  [P{self.proposer_id}] Phase 2: Accept({current_id}, '{current_proposal.value}') 요청.")

        accepts = 0
        
        # Acceptor들에게 Accept 요청
        for acceptor in self.acceptors:
            if random.random() > 0.1: # 10% 확률로 노드 실패/응답 없음 가정
                if acceptor.accept(self.proposer_id, current_proposal):
                    accepts += 1
        
        if accepts < quorum:
            print(f"  [P{self.proposer_id}] Accept 실패. 응답 수: {accepts}, 정족수: {quorum}. 재시도 필요.")
            return False
        
        # 3. Learner Phase (여기서는 Proposer가 Learner 역할도 겸함)
        print(f"  [P{self.proposer_id}] 합의 성공! 결정된 값: '{current_proposal.value}'")
        return True

# === 2. 시뮬레이션 실행 ===

# 3개의 은행 분산 스토리지 노드(Acceptor) 초기화
acceptors = [Acceptor(1), Acceptor(2), Acceptor(3)]
proposer = Proposer(proposer_id=10, acceptors=acceptors) # 루이스의 요청을 처리하는 하나의 Proposer

# 시뮬레이션 트랜잭션 목록
transactions = [
    "OPEN Louis 100.0",   # 계좌 개설 (100달러로 초기 입금)
    "DEPOSIT Louis 50.0",  # 50달러 입금
    "WITHDRAW Louis 30.0", # 30달러 출금
    "WITHDRAW Louis 20.0"  # 20달러 출금
]

print("--- PAXOS 기반 은행 거래 시뮬레이션 시작 (3개 노드) ---")

for tx in transactions:
    success = proposer.propose(tx)
    if not success:
        print(f"🚨 거래 '{tx}' 합의 실패. 다음 거래로 진행하지 않고 종료하거나 재시도해야 함. (여기서는 다음 거래로 진행)")

print("\n--- 시뮬레이션 결과 ---")
for acceptor in acceptors:
    print(acceptor)
