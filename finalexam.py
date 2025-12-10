import random
import time
from enum import Enum

# --- 네트워크 분할 시뮬레이션용 전역 상태 ---
NETWORK_STATUS = {}  # 예: {(1, 3): False, (3, 1): False, ...}


def set_network_partition(node_a, node_b, connected=True):
    """
    두 노드 사이의 네트워크 연결/단절을 설정한다.
    connected=False 이면 양방향 통신이 끊어진다.
    """
    NETWORK_STATUS[(node_a, node_b)] = connected
    NETWORK_STATUS[(node_b, node_a)] = connected


# --- 1. 노드 상태 정의 (Node States) ---
class State(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


# --- 2. Raft 노드 클래스 ---
class RaftNode:
    def __init__(self, node_id, cluster_nodes):
        self.id = node_id
        self.cluster_nodes = cluster_nodes  # 클러스터 내 모든 노드 ID 리스트

        # 지속 상태 (Persistent State)
        self.current_term = 0
        self.voted_for = None  # 현재 임기에서 투표한 노드 ID

        # 휘발성 상태 (Volatile State)
        self.state = State.FOLLOWER
        self.leader_id = None
        self.votes_received = 0

        # 로그 (각 항목: (command, term))
        self.log = []

        # 커밋/적용 인덱스 (Extra_1)
        self.commit_index = 0
        self.last_applied = 0

        # 리더 전용 복제 상태 (Extra_1)
        # 리더가 되었을 때 다시 초기화된다.
        self.next_index = {i: 1 for i in cluster_nodes}
        self.match_index = {i: 0 for i in cluster_nodes}

        # 시간 관리
        self.election_timeout = random.uniform(5.0, 10.0)  # 랜덤 선출 타임아웃
        self.last_heartbeat = time.time()

        # RPC 시뮬레이션을 위해 전체 노드 리스트를 나중에 주입
        self.cluster_nodes_obj = None

    # --- 네트워크 상태 확인 (Extra_2) ---
    def is_reachable(self, target_id):
        # 네트워크 연결 상태 확인 (기본은 True)
        return NETWORK_STATUS.get((self.id, target_id), True)

    # --- 3. 리더 선출 시작 ---
    def start_election(self):
        print(f"\n[{self.id}] 선출 시간 초과. 후보자로 전환합니다.")
        self.state = State.CANDIDATE
        self.current_term += 1
        self.voted_for = self.id  # 자신에게 투표
        self.votes_received = 1
        self.leader_id = None
        self.last_heartbeat = time.time()  # 타임아웃 재설정

        print(f"[{self.id}] 임기 {self.current_term}의 투표 요청을 보냅니다.")

        votes_needed = len(self.cluster_nodes) // 2 + 1

        # 다른 노드들에게 투표 요청 RPC 시뮬레이션
        for node_id in self.cluster_nodes:
            if node_id == self.id:
                continue

            vote_granted, term = self.request_vote(node_id)

            if term > self.current_term:
                # 더 큰 term 을 본 경우 바로 팔로워로 강등
                self.current_term = term
                self.state = State.FOLLOWER
                self.voted_for = None
                print(f"[{self.id}] 더 높은 임기 {term} 발견. 팔로워로 강등.")
                return  # 선거 중단

            if vote_granted and self.state == State.CANDIDATE:
                self.votes_received += 1
                print(
                    f"[{self.id}] Node {node_id}로부터 투표 획득. "
                    f"총 {self.votes_received}/{votes_needed}표."
                )

        # 투표 결과 확인
        if self.state == State.CANDIDATE and self.votes_received >= votes_needed:
            self.state = State.LEADER
            self.leader_id = self.id

            # 리더가 되면 로그 복제를 위한 next_index/match_index 초기화 (Extra_1)
            last_log_index = len(self.log)
            self.next_index = {i: last_log_index + 1 for i in self.cluster_nodes}
            self.match_index = {i: 0 for i in self.cluster_nodes}
            self.match_index[self.id] = last_log_index

            print(f"\n🎉🎉🎉 [{self.id}] 리더 당선! 임기 {self.current_term}. 🎉🎉🎉")
            # 리더 당선 후 바로 하트비트(= AppendEntries) 전송
            self.send_append_entries()
        elif self.state == State.CANDIDATE:
            print(f"\n[{self.id}] 과반수 득표 실패. 다음 선거 대기.")
            # 실제로는 랜덤 시간을 기다린 후 재선거 시작

    # --- 4. RequestVote RPC (다른 노드에게 요청) ---
    def request_vote(self, target_id):
        # 네트워크 분할로 도달 불가하면 실패로 간주 (Extra_2)
        if not self.is_reachable(target_id):
            return False, self.current_term

        target_node = next(
            (n for n in self.cluster_nodes_obj if n.id == target_id), None
        )
        if target_node:
            return target_node._handle_request_vote(
                self.current_term,
                self.id,
                len(self.log),
                self.log[-1][1] if self.log else 0,
            )
        return False, self.current_term

    # --- 5. RequestVote 핸들러 (내부 로직) ---
    def _handle_request_vote(self, term, candidate_id, last_log_index, last_log_term):
        # 1. 임기 확인
        if term < self.current_term:
            return False, self.current_term  # 오래된 임기는 거부

        if term > self.current_term:
            self.current_term = term
            self.state = State.FOLLOWER
            self.voted_for = None
            self.leader_id = None
            print(f"[{self.id}] 더 높은 임기 {term} 수신. 팔로워로 강등.")

        vote_granted = False

        # 2. 투표 자격 확인
        can_vote = self.voted_for is None or self.voted_for == candidate_id

        # 3. 후보자의 로그가 최소한 자신만큼 최신인지 검사 (간단 버전)
        my_last_term = self.log[-1][1] if self.log else 0
        my_last_index = len(self.log)

        log_up_to_date = (last_log_term > my_last_term) or (
            last_log_term == my_last_term and last_log_index >= my_last_index
        )

        if can_vote and log_up_to_date:
            self.voted_for = candidate_id
            self.last_heartbeat = time.time()  # 투표 후 타임아웃 재설정
            vote_granted = True
            print(f"[{self.id}] {candidate_id}에게 투표 승인 (Term {self.current_term}).")

        return vote_granted, self.current_term

    # --- 6. AppendEntries RPC (하트비트 / 로그 복제) 처리 (Extra_1) ---
    def handle_append_entries(
        self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit
    ):
        # (1) 임기 확인
        if term < self.current_term:
            return False, self.current_term

        # 리더의 term 이 더 크거나 같으면 팔로워로 전환
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            print(f"[{self.id}] 더 높은 임기 {term} 수신. 임기 업데이트.")

        self.state = State.FOLLOWER
        self.leader_id = leader_id
        self.last_heartbeat = time.time()

        # (2) 로그 일관성 검사
        if prev_log_index > len(self.log):
            return False, self.current_term

        if prev_log_index > 0:
            local_prev_term = self.log[prev_log_index - 1][1]
            if local_prev_term != prev_log_term:
                # prev_log_index 에 해당하는 term 이 다르면 불일치
                return False, self.current_term

        # (3) 충돌 로그 삭제 및 새 엔트리 추가
        if entries:
            # prev_log_index 이후의 로그를 모두 지우고
            if prev_log_index < len(self.log):
                self.log = self.log[:prev_log_index]

            # 새 엔트리 추가
            self.log.extend(entries)
            print(
                f"[{self.id}] 로그 {len(entries)}개 복제 완료. "
                f"현재 로그 길이: {len(self.log)}"
            )

        # (4) 커밋 인덱스 업데이트
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log))
            self._apply_logs()

        return True, self.current_term

    # --- 6-1. 커밋된 로그를 상태 머신에 적용 ---
    def _apply_logs(self):
        while self.commit_index > self.last_applied:
            self.last_applied += 1
            command, term = self.log[self.last_applied - 1]
            # 실제 상태 머신 적용 로직 대신 출력만 수행
            print(
                f"[{self.id}] 로그 인덱스 {self.last_applied} 적용(커밋). "
                f"command={command}, term={term}"
            )

    # --- 7. 리더의 AppendEntries 전송 (하트비트 + 로그 복제) ---
    def send_append_entries(self):
        if self.state != State.LEADER:
            return

        # 각 팔로워에게 자신의 로그를 전송
        for node_id in self.cluster_nodes:
            if node_id == self.id:
                continue

            # 네트워크 분할 시 통신 불가 노드는 건너뜀 (Extra_2)
            if not self.is_reachable(node_id):
                continue

            next_idx = self.next_index.get(node_id, 1)
            prev_idx = next_idx - 1

            prev_term = self.log[prev_idx - 1][1] if prev_idx > 0 else 0
            entries_to_send = self.log[prev_idx:]

            target_node = next(
                (n for n in self.cluster_nodes_obj if n.id == node_id), None
            )
            if not target_node:
                continue

            success, term = target_node.handle_append_entries(
                self.current_term,
                self.id,
                prev_idx,
                prev_term,
                entries_to_send,
                self.commit_index,
            )

            if term > self.current_term:
                # 더 큰 term 발견 시 즉시 팔로워로 강등
                self.current_term = term
                self.state = State.FOLLOWER
                self.voted_for = None
                self.leader_id = None
                print(f"[{self.id}] AppendEntries 응답에서 더 높은 임기 {term} 발견. 팔로워로 강등.")
                return

            if success:
                # 성공 시 next_index와 match_index 업데이트
                self.next_index[node_id] = len(self.log) + 1
                self.match_index[node_id] = len(self.log)
            else:
                # 실패 시 next_index를 줄여서 다시 시도
                self.next_index[node_id] = max(1, self.next_index[node_id] - 1)

        # 전송 후 과반수 복제 여부 확인
        self._check_commit_majority()

    # --- 7-1. 과반수 복제 여부 확인 및 커밋 (Extra_1) ---
    def _check_commit_majority(self):
        # 과반수 노드가 복제한 가장 높은 인덱스를 찾음
        matched = sorted(self.match_index.values(), reverse=True)
        # 노드 수가 N 이면, 과반수는 인덱스 N//2 위치
        majority_match_index = matched[len(self.cluster_nodes) // 2]

        if (
            majority_match_index > self.commit_index
            and majority_match_index > 0
            and self.log[majority_match_index - 1][1] == self.current_term
        ):
            self.commit_index = majority_match_index
            self._apply_logs()

    # --- 8. 메인 루프에서 노드 상태 체크 ---
    def check_status(self):
        now = time.time()
        if self.state in (State.FOLLOWER, State.CANDIDATE):
            # 선출 타임아웃 확인
            if now - self.last_heartbeat > self.election_timeout:
                self.start_election()

        elif self.state == State.LEADER:
            # 하트비트/로그 전송 간격 (예: 1초마다)
            if now - self.last_heartbeat > 1.0:
                # 간단한 데모를 위해 리더가 주기적으로 새로운 커맨드를 추가
                command = f"set_x={int(now)}"
                self.log.append((command, self.current_term))
                print(f"[{self.id}] 리더가 새 로그 추가: {command}")

                self.send_append_entries()
                self.last_heartbeat = now


# --- 시뮬레이션 유틸리티 함수 ---
def run_simulation_step(duration, nodes, desc=None):
    if desc:
        print(desc)

    start_time = time.time()
    while time.time() - start_time < duration:
        for node in nodes:
            node.check_status()

        time.sleep(0.5)

        leaders = [n.id for n in nodes if n.state == State.LEADER]
        if len(leaders) > 1:
            print(
                "\n🚨🚨🚨 오류: 리더가 2명 이상입니다! "
                f"(Safety Property 위반) leaders={leaders} 🚨🚨🚨"
            )
            return


# --- 시뮬레이션 실행 (기본 + 네트워크 분할 테스트) ---
if __name__ == "__main__":
    NODE_COUNT = 5
    CLUSTER_IDS = list(range(1, NODE_COUNT + 1))

    # 노드 객체 생성
    nodes = [RaftNode(i, CLUSTER_IDS) for i in CLUSTER_IDS]

    # 노드 객체 리스트를 각 노드 인스턴스에 저장 (RPC 시뮬레이션을 위해 필요)
    for node in nodes:
        node.cluster_nodes_obj = nodes

    print("--- Raft 시뮬레이션 시작 (5개 노드) ---")

    # 초기 선거를 위한 랜덤 타이머 설정 (각 노드의 타이머는 다름)
    for node in nodes:
        node.last_heartbeat -= random.uniform(0, 10)
        print(f"Node {node.id}: 초기 임기 {node.current_term}, 상태 {node.state.name}")

    # 1단계: 정상 상태에서 잠시 실행
    run_simulation_step(10, nodes, "\n--- 1단계: 정상 상태 시뮬레이션 (10초) ---")

    # 2단계: 네트워크 분할 시뮬레이션 (Extra_2_simulation)
    print("\n--- 시뮬레이션: 네트워크 분할 테스트 ---")

    # 5개 노드 중 {1, 2} 와 {3, 4, 5}로 분할
    for a in (1, 2):
        for b in (3, 4, 5):
            set_network_partition(a, b, False)

    print("!!! 네트워크 분할: {1, 2} vs {3, 4, 5} !!!")

    run_simulation_step(10, nodes, "\n--- 2단계: 분할된 상태로 10초 실행 ---")

    # 3단계: 분할 복구
    for a in (1, 2):
        for b in (3, 4, 5):
            set_network_partition(a, b, True)

    print("!!! 네트워크 복구 !!!")

    run_simulation_step(10, nodes, "\n--- 3단계: 복구 후 10초 실행 ---")

    print("\n--- 시뮬레이션 종료 ---")
    for node in nodes:
        print(
            f"Node {node.id}: 최종 상태 {node.state.name}, "
            f"임기 {node.current_term}, 리더 ID {node.leader_id}"
        )
