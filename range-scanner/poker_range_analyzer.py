#!/usr/bin/env python3
"""
Poker Hand Range Analyzer
Parses hand history files and analyzes ranges by position, bet sizing, and hand stage
"""

import re
import duckdb
import pyarrow as pa
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from statistics import median
from multiprocessing import Pool, cpu_count


@dataclass
class HandAction:
    """Represents a single action in a poker hand"""

    player: str
    action_type: str  # fold, call, raise, bet, check, etc.
    amount: float  # Raw chip amount
    position: str  # SB, BB, BTN, CO, MP, EP, etc.
    stage: str  # preflop, flop, turn, river
    pot_before: float = 0
    stack_size: float = 0
    amount_bb: float = 0  # Amount in big blinds
    pot_odds: float = 0  # Amount relative to pot (X times pot)
    bb_size: float = 0
    tournament_stage: str = "start"


@dataclass
class PlayerHand:
    """Represents a player's shown cards"""

    player: str
    cards: str  # e.g., "Ac Th"
    position: str
    actions: List[HandAction]
    tournament_id: str
    hand_id: str
    chunk_index: int
    order_index: int
    source_file: str
    bb_size: float


@dataclass
class RangeOccurrence:
    """Flat representation of a single range event"""

    tournament_id: str
    chunk_index: int
    hand_id: str
    order_index: int
    player: str
    position: str
    stage: str
    action_type: str
    cards: str
    tournament_stage: str
    action_amount: float
    pot_before: float
    stack_size: float
    bb_size: float
    amount_bb: float
    pot_odds: float
    pot_bucket: str
    bb_bucket: str
    stack_bucket: str
    stack_size_bb: float
    showdown: bool
    source_file: str


class HandHistoryParser:
    """Parses PokerStars hand history files"""

    # Position mapping based on seat relative to button
    POSITION_NAMES = {
        0: "BTN",  # Button
        1: "SB",  # Small Blind
        2: "BB",  # Big Blind
        3: "EP",  # Early Position
        4: "MP",  # Middle Position
        5: "CO",  # Cutoff
        6: "EP",
        7: "EP",
        8: "EP",
    }

    # Pre-compiled regex patterns for better performance
    BUTTON_PATTERN = re.compile(r"Seat #(\d+) is the button")
    BB_PATTERN = re.compile(r"Hold\'em No Limit - Level [IVXL]+ \((\d+)/(\d+)\)")
    SEAT_PATTERN = re.compile(r"Seat (\d+): ([^\s]+) \((\d+) in chips\)")
    STAGE_PATTERN = re.compile(r"\*\*\* (FLOP|TURN|RIVER) \*\*\*")
    SHOWN_PATTERN = re.compile(r"Seat \d+: ([^\s]+) .*showed \[([^\]]+)\]")
    TOURNAMENT_FILE_PATTERN = re.compile(r"hhDealer\.com_(\d+)-(\d+)_")
    HAND_ID_PATTERN = re.compile(r"PokerStars Hand #(\d+)")
    LEVEL_PATTERN = re.compile(r"Level ([IVXL]+)")
    PAYOUT_PATTERN = re.compile(r"finished the tournament in (\d+)[^\n]*received \$")
    FINISH_PATTERN = re.compile(r"finished the tournament in (\d+)")

    # Pre-compiled action patterns
    ACTION_PATTERNS = [
        (re.compile(r"([^\s:]+): raises (\d+\.?\d*) to (\d+\.?\d*)"), "raise"),
        (re.compile(r"([^\s:]+): bets (\d+\.?\d*)"), "bet"),
        (re.compile(r"([^\s:]+): calls (\d+\.?\d*)"), "call"),
        (re.compile(r"([^\s:]+): folds"), "fold"),
        (re.compile(r"([^\s:]+): checks"), "check"),
    ]

    def __init__(self):
        self.hands_parsed = 0

    def normalize_card_notation(self, cards: str) -> str:
        """Convert cards to normalized format: e.g., 'Ac Th' -> 'AcTh' -> 'ATo'"""
        if not cards or len(cards) < 4:
            return ""

        # Extract rank and suit
        try:
            card1_rank, card1_suit = cards[0], cards[1]
            card2_rank, card2_suit = cards[3], cards[4]

            # Sort by rank (high to low)
            rank_order = "AKQJT98765432"
            if rank_order.index(card1_rank) > rank_order.index(card2_rank):
                card1_rank, card2_rank = card2_rank, card1_rank
                card1_suit, card2_suit = card2_suit, card1_suit

            # Suited or offsuit
            if card1_suit == card2_suit:
                suffix = "s"
            elif card1_rank == card2_rank:
                suffix = ""  # Pocket pair
            else:
                suffix = "o"

            return f"{card1_rank}{card2_rank}{suffix}"
        except (IndexError, ValueError):
            return ""

    def get_position(self, seat_num: int, button_seat: int, total_seats: int) -> str:
        """Calculate position based on seat number and button

        Returns position with table size context:
        - EP(6) / MP(6) = Early/Middle Position at 6-max tables
        - EP(7+) / MP(7+) = Early/Middle Position at 7+ handed tables

        Position mapping logic:
        - seats_after_button = 0: BTN (button)
        - seats_after_button = 1: SB (small blind)
        - seats_after_button = 2: BB (big blind)
        - seats_after_button = 3+: EP or MP depending on table size
        - seats_after_button = (total_seats - 1): CO (cutoff)
        """
        if total_seats <= 0:
            return "UNKNOWN"

        # Calculate seats after button
        seats_after_button = (seat_num - button_seat) % total_seats

        # Map to position names with table size context
        if total_seats == 2:  # Heads-up
            pos_map = {0: "BTN", 1: "BB"}
        elif total_seats == 3:  # 3-handed
            pos_map = {0: "BTN", 1: "SB", 2: "BB"}
        elif total_seats == 4:  # 4-handed
            pos_map = {0: "BTN", 1: "SB", 2: "BB", 3: "CO"}
        elif total_seats == 5:  # 5-handed
            pos_map = {0: "BTN", 1: "SB", 2: "BB", 3: "EP(6)", 4: "CO"}
        elif total_seats == 6:  # 6-max
            pos_map = {0: "BTN", 1: "SB", 2: "BB", 3: "EP(6)", 4: "MP(6)", 5: "CO"}
        else:  # 7+ handed (includes 7, 8, 9, 10+)
            # All 7+ tables use EP(7+) and MP(7+)
            if total_seats == 7:
                pos_map = {
                    0: "BTN",
                    1: "SB",
                    2: "BB",
                    3: "EP(7+)",
                    4: "EP(7+)",
                    5: "MP(7+)",
                    6: "CO",
                }
            elif total_seats == 8:
                pos_map = {
                    0: "BTN",
                    1: "SB",
                    2: "BB",
                    3: "EP(7+)",
                    4: "EP(7+)",
                    5: "MP(7+)",
                    6: "MP(7+)",
                    7: "CO",
                }
            elif total_seats == 9:
                pos_map = {
                    0: "BTN",
                    1: "SB",
                    2: "BB",
                    3: "EP(7+)",
                    4: "EP(7+)",
                    5: "EP(7+)",
                    6: "MP(7+)",
                    7: "MP(7+)",
                    8: "CO",
                }
            else:  # 10+
                pos_map = {
                    0: "BTN",
                    1: "SB",
                    2: "BB",
                    3: "EP(7+)",
                    4: "EP(7+)",
                    5: "EP(7+)",
                    6: "EP(7+)",
                    7: "MP(7+)",
                    8: "MP(7+)",
                    9: "CO",
                }

        return pos_map.get(seats_after_button, "UNKNOWN")

    @staticmethod
    def tournament_info_from_path(file_path: str) -> Tuple[str, int]:
        """Extract tournament ID and chunk index from file name"""
        name = Path(file_path).name
        match = HandHistoryParser.TOURNAMENT_FILE_PATTERN.search(name)
        if match:
            tournament_id, chunk = match.groups()
            return tournament_id, int(chunk)
        # Fallback: use entire file name as ID
        return name, 0

    def parse_tournament(
        self, file_entries: List[Tuple[int, str]], tournament_id: str
    ) -> List[PlayerHand]:
        """Parse all files for a single tournament, applying stage classification"""
        hand_entries = []
        order_index = 0

        for chunk_index, file_path in file_entries:
            try:
                with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
            except Exception:
                continue

            hands = re.split(r"\n\n+", content)
            for hand_text in hands:
                if not hand_text.strip():
                    continue

                hand_id_match = self.HAND_ID_PATTERN.search(hand_text)
                hand_id = (
                    hand_id_match.group(1)
                    if hand_id_match
                    else f"{tournament_id}_{order_index}"
                )
                level_match = self.LEVEL_PATTERN.search(hand_text)
                level_label = level_match.group(1) if level_match else "UNKNOWN"

                hand_entries.append(
                    {
                        "id": hand_id,
                        "text": hand_text,
                        "level": level_label,
                        "order": order_index,
                        "chunk": chunk_index,
                        "source_file": file_path,
                    }
                )
                order_index += 1
                self.hands_parsed += 1

        stage_map = self._build_stage_map(hand_entries)
        shown_hands = []

        for entry in hand_entries:
            stage_label = stage_map.get(entry["id"], "start")
            try:
                parsed = self.parse_single_hand(
                    hand_text=entry["text"],
                    tournament_stage=stage_label,
                    tournament_id=tournament_id,
                    hand_id=entry["id"],
                    chunk_index=entry["chunk"],
                    order_index=entry["order"],
                    source_file=entry["source_file"],
                )
                if parsed:
                    shown_hands.extend(parsed)
            except Exception:
                continue

        return shown_hands

    def _build_stage_map(self, hand_entries: List[Dict[str, str]]) -> Dict[str, str]:
        """Classify tournament stages for each hand"""
        stage_map: Dict[str, str] = {}
        if not hand_entries:
            return stage_map

        levels_in_order: List[str] = []
        level_seen = set()
        first_payout_index: Optional[int] = None
        bubble_level: Optional[str] = None
        first_final_table_index: Optional[int] = None

        for entry in hand_entries:
            level = entry["level"]
            if level not in level_seen:
                levels_in_order.append(level)
                level_seen.add(level)

            text = entry["text"]
            order_idx = entry["order"]

            if first_payout_index is None and self.PAYOUT_PATTERN.search(text):
                first_payout_index = order_idx
                bubble_level = level

            if first_final_table_index is None:
                for match in self.FINISH_PATTERN.finditer(text):
                    place = int(match.group(1))
                    if place <= 9:
                        first_final_table_index = order_idx
                        break

        pre_bubble_levels: List[str] = []
        if bubble_level and bubble_level in levels_in_order:
            bubble_pos = levels_in_order.index(bubble_level)
            start_idx = max(0, bubble_pos - 3)
            pre_bubble_levels = levels_in_order[start_idx:bubble_pos]

        for entry in hand_entries:
            order_idx = entry["order"]
            level = entry["level"]
            stage = "start"

            if (
                first_final_table_index is not None
                and order_idx >= first_final_table_index
            ):
                stage = "final_table"
            elif (
                first_payout_index is not None
                and bubble_level
                and level == bubble_level
                and order_idx >= first_payout_index
            ):
                stage = "bubble"
            elif (
                first_payout_index is not None
                and level in pre_bubble_levels
                and order_idx < first_payout_index
            ):
                stage = "pre_bubble"

            stage_map[entry["id"]] = stage

        return stage_map

    def parse_single_hand(
        self,
        hand_text: str,
        tournament_stage: str = "start",
        tournament_id: str = "",
        hand_id: str = "",
        chunk_index: int = 0,
        order_index: int = 0,
        source_file: str = "",
    ) -> List[PlayerHand]:
        """Parse a single hand from text"""
        lines = hand_text.strip().split("\n")
        if len(lines) < 5:
            return []

        # Extract button position using pre-compiled pattern
        button_match = self.BUTTON_PATTERN.search(hand_text)
        if not button_match:
            return []
        button_seat = int(button_match.group(1))

        # Extract big blind size using pre-compiled pattern
        bb_match = self.BB_PATTERN.search(hand_text)
        if bb_match:
            bb_size = float(bb_match.group(2))
        else:
            bb_size = 1.0  # Default if not found

        # Extract all players and their seat numbers using pre-compiled pattern
        players = {}
        for match in self.SEAT_PATTERN.finditer(hand_text):
            seat, player_name, chips = match.groups()
            players[player_name] = {"seat": int(seat), "chips": int(chips)}

        total_seats = len(players)

        # Parse actions by stage
        current_stage = "preflop"

        actions_by_player = defaultdict(list)

        # Track pot size
        pot_size = 0.0

        for line in lines:
            # Detect stage changes using pre-compiled pattern
            stage_match = self.STAGE_PATTERN.search(line)
            if stage_match:
                current_stage = stage_match.group(1).lower()
                continue

            # Parse actions using pre-compiled patterns
            for pattern, action_type in self.ACTION_PATTERNS:
                match = pattern.search(line)
                if match:
                    player_name = match.group(1)

                    if player_name not in players:
                        continue

                    # Get position
                    position = self.get_position(
                        players[player_name]["seat"], button_seat, total_seats
                    )

                    # Extract amount
                    if action_type in ["raise", "bet", "call"]:
                        if action_type == "raise":
                            amount = float(match.group(3))
                        else:
                            amount = float(match.group(2))
                    else:
                        amount = 0.0

                    # Calculate BB and pot-relative sizing
                    amount_bb = amount / bb_size if bb_size > 0 else 0
                    pot_odds = amount / pot_size if pot_size > 0 else 0

                    action = HandAction(
                        player=player_name,
                        action_type=action_type,
                        amount=amount,
                        position=position,
                        stage=current_stage,
                        pot_before=pot_size,
                        stack_size=players[player_name]["chips"],
                        amount_bb=amount_bb,
                        pot_odds=pot_odds,
                        bb_size=bb_size,
                        tournament_stage=tournament_stage,
                    )

                    actions_by_player[player_name].append(action)

                    if action_type in ["raise", "bet", "call"]:
                        pot_size += amount

                    break

        # Parse shown hands using pre-compiled pattern
        shown_hands = []

        for match in self.SHOWN_PATTERN.finditer(hand_text):
            player_name, cards = match.groups()

            if player_name not in players:
                continue

            position = self.get_position(
                players[player_name]["seat"], button_seat, total_seats
            )

            normalized_cards = self.normalize_card_notation(cards)

            if normalized_cards:
                player_hand = PlayerHand(
                    player=player_name,
                    cards=normalized_cards,
                    position=position,
                    actions=actions_by_player.get(player_name, []),
                    tournament_id=tournament_id,
                    hand_id=hand_id,
                    chunk_index=chunk_index,
                    order_index=order_index,
                    source_file=source_file,
                    bb_size=bb_size,
                )
                shown_hands.append(player_hand)

        return shown_hands


class RangeAnalyzer:
    """Collects normalized range occurrences"""

    def __init__(self):
        self.occurrences: List[RangeOccurrence] = []
        self.total_actions = 0
        self.total_shown_hands = 0

    def categorize_bet_size(self, action: HandAction) -> str:
        """Categorize bet sizing relative to pot"""
        if action.action_type not in ["raise", "bet"]:
            return "N/A"

        if action.pot_before == 0:
            return "OPEN"

        ratio = action.pot_odds  # Already calculated as amount/pot

        if ratio < 0.33:
            return "<0.33x"
        elif ratio < 0.50:
            return "0.33x"
        elif ratio < 0.75:
            return "0.5x"
        elif ratio < 1.0:
            return "0.75x"
        elif ratio < 1.5:
            return "1x"
        elif ratio < 2.0:
            return "1.5x"
        elif ratio < 3.0:
            return "2x"
        else:
            return "3x+"

    def categorize_bb_size(self, action: HandAction) -> str:
        """Categorize bet sizing in big blinds"""
        if action.action_type not in ["raise", "bet", "call"]:
            return "N/A"

        bb = action.amount_bb

        if action.stage == "preflop":
            if action.action_type == "raise":
                if bb < 2.5:
                    return "MINRAISE"
                elif bb < 3.0:
                    return "2.5BB"
                elif bb < 4.0:
                    return "3BB"
                elif bb < 6.0:
                    return "4-5BB"
                elif bb < 10.0:
                    return "6-9BB"
                else:
                    return "10BB+"
            elif action.action_type == "call":
                if bb < 2.0:
                    return "1BB_CALL"
                elif bb < 3.0:
                    return "2BB_CALL"
                elif bb < 5.0:
                    return "3-4BB_CALL"
                else:
                    return "5BB+_CALL"
        else:
            # Postflop in BB
            if bb < 1.0:
                return "<1BB"
            elif bb < 3.0:
                return "1-3BB"
            elif bb < 6.0:
                return "3-6BB"
            elif bb < 10.0:
                return "6-10BB"
            else:
                return "10BB+"

        return "OTHER"

    def categorize_stack_size(self, stack_bb: float) -> str:
        """Bucketize player stack depth in big blinds"""
        if stack_bb <= 0:
            return "UNKNOWN"
        if stack_bb < 10:
            return "<10BB"
        if stack_bb < 20:
            return "10-20BB"
        if stack_bb < 30:
            return "20-30BB"
        if stack_bb < 50:
            return "30-50BB"
        if stack_bb < 80:
            return "50-80BB"
        return "80BB+"

    def add_hand(self, player_hand: PlayerHand):
        """Record all shown actions as flat occurrences"""
        self.total_shown_hands += 1
        cards = player_hand.cards
        for action in player_hand.actions:
            position = action.position or player_hand.position
            stage = action.stage
            action_type = action.action_type

            pot_category = (
                self.categorize_bet_size(action)
                if action_type in ["raise", "bet"]
                else "N/A"
            )
            bb_category = (
                self.categorize_bb_size(action)
                if action_type in ["raise", "bet", "call"]
                else "N/A"
            )

            stack_bb = action.stack_size / action.bb_size if action.bb_size > 0 else 0
            stack_bucket = self.categorize_stack_size(stack_bb)

            occurrence = RangeOccurrence(
                tournament_id=player_hand.tournament_id,
                chunk_index=player_hand.chunk_index,
                hand_id=player_hand.hand_id,
                order_index=player_hand.order_index,
                player=action.player,
                position=position,
                stage=stage,
                action_type=action_type,
                cards=cards,
                tournament_stage=action.tournament_stage,
                action_amount=action.amount,
                pot_before=action.pot_before,
                stack_size=action.stack_size,
                bb_size=action.bb_size,
                amount_bb=action.amount_bb,
                pot_odds=action.pot_odds,
                pot_bucket=pot_category or "N/A",
                bb_bucket=bb_category or "N/A",
                stack_bucket=stack_bucket,
                stack_size_bb=stack_bb,
                showdown=True,
                source_file=player_hand.source_file,
            )
            self.occurrences.append(occurrence)
            self.total_actions += 1


class RangeDatabaseExporter:
    """Writes range occurrences into a denormalized DuckDB warehouse."""

    BATCH_SIZE = 200_000

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)

    def export(self, occurrences: List[RangeOccurrence]):
        if self.db_path.exists():
            self.db_path.unlink()

        conn = duckdb.connect(self.db_path.as_posix())

        try:
            self._create_schema(conn)
            self._populate(conn, occurrences)
        finally:
            conn.close()

        print(f"Exported analysis to {self.db_path}")

    def _create_schema(self, conn: duckdb.DuckDBPyConnection):
        conn.execute("DROP TABLE IF EXISTS range_occurrences")
        conn.execute(
            """
            CREATE TABLE range_occurrences (
                tournament_id TEXT,
                hand_id TEXT,
                chunk_index INTEGER,
                order_index INTEGER,
                player TEXT,
                position TEXT,
                stage TEXT,
                action TEXT,
                cards TEXT,
                tournament_stage TEXT,
                pot_bucket TEXT,
                bb_bucket TEXT,
                stack_bucket TEXT,
                action_amount DOUBLE,
                pot_before DOUBLE,
                stack_size DOUBLE,
                stack_size_bb DOUBLE,
                bb_size DOUBLE,
                amount_bb DOUBLE,
                pot_odds DOUBLE,
                showdown BOOLEAN,
                source_file TEXT
            )
            """
        )

    def _create_indexes(self, conn: duckdb.DuckDBPyConnection):
        conn.execute(
            """
            CREATE INDEX idx_range_key
            ON range_occurrences(position, stage, action)
            """
        )
        conn.execute(
            """
            CREATE INDEX idx_range_buckets
            ON range_occurrences(pot_bucket, bb_bucket, stack_bucket)
            """
        )
        conn.execute(
            """
            CREATE INDEX idx_range_stage
            ON range_occurrences(tournament_stage)
            """
        )

    def _populate(
        self, conn: duckdb.DuckDBPyConnection, occurrences: List[RangeOccurrence]
    ):
        if not occurrences:
            return

        batch = self._empty_batch()
        batch_count = 0

        for occ in occurrences:
            self._append_to_batch(batch, occ)
            batch_count += 1
            if batch_count >= self.BATCH_SIZE:
                self._insert_batch(conn, batch)
                batch = self._empty_batch()
                batch_count = 0

        if batch["tournament_id"]:
            self._insert_batch(conn, batch)

        self._create_indexes(conn)

    @staticmethod
    def _empty_batch() -> Dict[str, List]:
        return {
            "tournament_id": [],
            "hand_id": [],
            "chunk_index": [],
            "order_index": [],
            "player": [],
            "position": [],
            "stage": [],
            "action": [],
            "cards": [],
            "tournament_stage": [],
            "pot_bucket": [],
            "bb_bucket": [],
            "stack_bucket": [],
            "action_amount": [],
            "pot_before": [],
            "stack_size": [],
            "stack_size_bb": [],
            "bb_size": [],
            "amount_bb": [],
            "pot_odds": [],
            "showdown": [],
            "source_file": [],
        }

    @staticmethod
    def _append_to_batch(batch: Dict[str, List], occ: RangeOccurrence):
        batch["tournament_id"].append(occ.tournament_id)
        batch["hand_id"].append(occ.hand_id)
        batch["chunk_index"].append(occ.chunk_index)
        batch["order_index"].append(occ.order_index)
        batch["player"].append(occ.player)
        batch["position"].append(occ.position)
        batch["stage"].append(occ.stage)
        batch["action"].append(occ.action_type)
        batch["cards"].append(occ.cards)
        batch["tournament_stage"].append(occ.tournament_stage)
        batch["pot_bucket"].append(occ.pot_bucket)
        batch["bb_bucket"].append(occ.bb_bucket)
        batch["stack_bucket"].append(occ.stack_bucket)
        batch["action_amount"].append(occ.action_amount)
        batch["pot_before"].append(occ.pot_before)
        batch["stack_size"].append(occ.stack_size)
        batch["stack_size_bb"].append(occ.stack_size_bb)
        batch["bb_size"].append(occ.bb_size)
        batch["amount_bb"].append(occ.amount_bb)
        batch["pot_odds"].append(occ.pot_odds)
        batch["showdown"].append(bool(occ.showdown))
        batch["source_file"].append(occ.source_file)

    @staticmethod
    def _insert_batch(conn: duckdb.DuckDBPyConnection, batch: Dict[str, List]):
        table = pa.Table.from_pydict(batch)
        conn.register("occ_batch", table)
        conn.execute("INSERT INTO range_occurrences SELECT * FROM occ_batch")
        conn.unregister("occ_batch")


class RangeReportBuilder:
    """Builds human-readable reports directly from the DuckDB warehouse"""

    POSITIONS = ["BTN", "SB", "BB", "CO", "MP(6)", "MP(7+)", "EP(6)", "EP(7+)"]
    STAGES = ["preflop", "flop", "turn", "river"]
    ACTIONS = ["raise", "bet", "call", "check", "fold"]
    POT_ORDER = ["OPEN", "<0.33x", "0.33x", "0.5x", "0.75x", "1x", "1.5x", "2x", "3x+"]
    BB_ORDER_PRE = [
        "MINRAISE",
        "2.5BB",
        "3BB",
        "4-5BB",
        "6-9BB",
        "10BB+",
        "1BB_CALL",
        "2BB_CALL",
        "3-4BB_CALL",
        "5BB+_CALL",
    ]
    BB_ORDER_POST = ["<1BB", "1-3BB", "3-6BB", "6-10BB", "10BB+"]
    TOURNAMENT_STAGES = ["start", "pre_bubble", "bubble", "final_table"]
    RANK_ORDER = "AKQJT98765432"

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database {self.db_path} not found.")

    def generate_report(self) -> str:
        report: List[str] = []
        report.append("=" * 80)
        report.append("POKER RANGE ANALYSIS REPORT")
        report.append("=" * 80)
        report.append("")

        with duckdb.connect(self.db_path.as_posix()) as conn:
            for position in self.POSITIONS:
                position_lines: List[str] = []
                for stage in self.STAGES:
                    stage_lines: List[str] = []
                    for action in self.ACTIONS:
                        action_data = self._fetch_action_data(
                            conn, position, stage, action
                        )
                        if not action_data:
                            continue
                        stage_lines.extend(
                            self._format_action_block(action, action_data)
                        )
                    if stage_lines:
                        position_lines.append(f"\n{stage.upper()}:")
                        position_lines.append("-" * 80)
                        position_lines.extend(stage_lines)
                if position_lines:
                    report.append("\n" + "=" * 80)
                    report.append(f"POSITION: {position}")
                    report.append("=" * 80)
                    report.extend(position_lines)

        return "\n".join(report)

    def preflop_open_summary(self) -> List[Tuple[str, int, int]]:
        results: Dict[str, Tuple[int, int]] = {}
        query = """
            SELECT position, COUNT(DISTINCT cards) AS unique_combos, COUNT(*) AS total_instances
            FROM range_occurrences
            WHERE stage = 'preflop' AND action = 'raise'
            GROUP BY position
        """
        with duckdb.connect(self.db_path.as_posix()) as conn:
            for name, unique_combos, total in conn.execute(query).fetchall():
                results[name] = (unique_combos, total)

        summary = []
        for position in self.POSITIONS:
            if position in results:
                unique_combos, total = results[position]
                summary.append((position, unique_combos, total))
        return summary

    def _fetch_action_data(
        self, conn: duckdb.DuckDBPyConnection, position: str, stage: str, action: str
    ):
        base_params = (position, stage, action)
        combos = self._query_combos(conn, base_params)
        if not combos:
            return None
        total_instances = sum(count for _, count in combos)
        median_pct = self._median_frequency_pct(
            [count for _, count in combos], total_instances
        )

        return {
            "position": position,
            "stage": stage,
            "action": action,
            "hands": combos,
            "total": total_instances,
            "median_pct": median_pct,
            "by_pot_size": self._query_bucket(
                conn, base_params, "COALESCE(pot_bucket, 'N/A')"
            ),
            "by_bb_size": self._query_bucket(
                conn, base_params, "COALESCE(bb_bucket, 'N/A')"
            ),
            "by_tournament_stage": self._query_bucket(
                conn, base_params, "COALESCE(tournament_stage, 'UNKNOWN')"
            ),
        }

    def _query_combos(
        self, conn: duckdb.DuckDBPyConnection, params: Tuple[str, str, str]
    ):
        query = """
            SELECT cards, COUNT(*) AS count
            FROM range_occurrences
            WHERE position = ? AND stage = ? AND action = ?
            GROUP BY cards
        """
        rows = conn.execute(query, params).fetchall()
        return sorted(rows, key=lambda row: self._hand_rank(row[0]))

    def _query_bucket(
        self,
        conn: duckdb.DuckDBPyConnection,
        params: Tuple[str, str, str],
        bucket_expr: str,
    ):
        query = f"""
            SELECT {bucket_expr} AS bucket, cards, COUNT(*) AS count
            FROM range_occurrences
            WHERE position = ? AND stage = ? AND action = ?
            GROUP BY bucket, cards
        """
        bucket_map: Dict[str, List[Tuple[str, int]]] = {}
        for bucket, hand, count in conn.execute(query, params).fetchall():
            bucket_map.setdefault(bucket, []).append((hand, count))
        for bucket in bucket_map:
            bucket_map[bucket] = sorted(
                bucket_map[bucket], key=lambda row: self._hand_rank(row[0])
            )
        return bucket_map

    def _format_action_block(self, action: str, data: Dict) -> List[str]:
        lines = []
        hands_str = ", ".join(f"{hand}({count})" for hand, count in data["hands"])
        lines.append(f"\n  {action.upper()}: {hands_str}")
        lines.append(
            f"    Total: {len(data['hands'])} unique combos, {data['total']} instances, "
            f"median combo frequency: {data['median_pct']:.2f}%"
        )
        if data["by_pot_size"]:
            lines.append("    By Pot Size:")
            for bucket in self.POT_ORDER + sorted(
                set(data["by_pot_size"]) - set(self.POT_ORDER)
            ):
                if bucket in data["by_pot_size"]:
                    lines.append(
                        self._format_bucket_line(
                            bucket, data["by_pot_size"][bucket], data["total"]
                        )
                    )
        if data["by_bb_size"]:
            lines.append("    By Big Blinds:")
            bb_order = (
                self.BB_ORDER_PRE if data["stage"] == "preflop" else self.BB_ORDER_POST
            )
            custom = sorted(set(data["by_bb_size"]) - set(bb_order))
            for bucket in bb_order + custom:
                if bucket in data["by_bb_size"]:
                    lines.append(
                        self._format_bucket_line(
                            bucket, data["by_bb_size"][bucket], data["total"]
                        )
                    )
        if data["by_tournament_stage"]:
            lines.append("    By Tournament Stage:")
            stage_order = self.TOURNAMENT_STAGES + sorted(
                set(data["by_tournament_stage"]) - set(self.TOURNAMENT_STAGES)
            )
            for bucket in stage_order:
                if bucket in data["by_tournament_stage"]:
                    stage_lines = data["by_tournament_stage"][bucket]
                    stage_total = sum(count for _, count in stage_lines)
                    hands_str = ", ".join(
                        f"{hand}({count})" for hand, count in stage_lines
                    )
                    freq_pct = (
                        (stage_total / data["total"] * 100) if data["total"] else 0
                    )
                    lines.append(
                        f"      {bucket}: {hands_str} [{stage_total} instances, {freq_pct:.1f}%]"
                    )
        return lines

    def _format_bucket_line(
        self, bucket: str, rows: List[Tuple[str, int]], total: int
    ) -> str:
        bucket_total = sum(count for _, count in rows)
        hands_str = ", ".join(f"{hand}({count})" for hand, count in rows)
        freq_pct = (bucket_total / total * 100) if total else 0
        return (
            f"      {bucket}: {hands_str} [{bucket_total} instances, {freq_pct:.1f}%]"
        )

    def _hand_rank(self, hand: str) -> int:
        if len(hand) == 2:
            return self.RANK_ORDER.index(hand[0]) * -100
        high = self.RANK_ORDER.index(hand[0])
        low = self.RANK_ORDER.index(hand[1])
        return high * -10 + low

    def _median_frequency_pct(self, counts: List[int], total: int) -> float:
        if not counts or total == 0:
            return 0.0
        med = median(counts)
        return (med / total) * 100


def process_tournament_worker(args):
    """Parse a tournament and return raw occurrences for multiprocessing."""
    tournament_id, file_entries = args
    parser = HandHistoryParser()
    analyzer = RangeAnalyzer()
    shown_hands = parser.parse_tournament(file_entries, tournament_id)
    for hand in shown_hands:
        analyzer.add_hand(hand)
    return {
        "tournament_id": tournament_id,
        "occurrences": analyzer.occurrences,
        "hands_parsed": parser.hands_parsed,
        "shown_hands": analyzer.total_shown_hands,
        "actions": analyzer.total_actions,
    }


def process_tournaments(
    tasks: List[Tuple[str, List[Tuple[int, str]]]], worker_count: int
):
    """Process all tournaments, optionally in parallel."""
    total = len(tasks)
    occurrences: List[RangeOccurrence] = []
    total_hands_parsed = 0
    total_shown_hands = 0
    total_actions = 0

    if total == 0:
        return occurrences, 0, 0, 0

    def handle_result(idx: int, result: Dict):
        nonlocal occurrences, total_hands_parsed, total_shown_hands, total_actions
        occurrences.extend(result["occurrences"])
        total_hands_parsed += result["hands_parsed"]
        total_shown_hands += result["shown_hands"]
        total_actions += result["actions"]
        if idx % 50 == 0 or idx == total:
            print(f"  Processed {idx}/{total} tournaments...")

    if worker_count > 1:
        print(f"\nProcessing with {worker_count} worker processes...")
        try:
            with Pool(worker_count) as pool:
                for idx, result in enumerate(
                    pool.imap_unordered(process_tournament_worker, tasks), start=1
                ):
                    handle_result(idx, result)
        except PermissionError:
            print(
                "  Multiprocessing unavailable in this environment, falling back to sequential mode."
            )
            worker_count = 1

    if worker_count == 1:
        print("\nProcessing tournaments sequentially...")
        for idx, task in enumerate(tasks, start=1):
            result = process_tournament_worker(task)
            handle_result(idx, result)

    return occurrences, total_hands_parsed, total_shown_hands, total_actions


def main():
    """Main entry point"""
    print("Poker Range Analyzer")
    print("=" * 80)

    hands_dir = Path("hands")
    if not hands_dir.exists():
        print(f"Error: {hands_dir} directory not found")
        return

    # Find all hand history files
    txt_files = list(hands_dir.rglob("*.txt"))
    print(f"Found {len(txt_files)} hand history files")

    if len(txt_files) == 0:
        print("No hand history files found!")
        return

    grouped_files: Dict[str, List[Tuple[int, Path]]] = {}
    for file_path in txt_files:
        tournament_id, chunk = HandHistoryParser.tournament_info_from_path(
            str(file_path)
        )
        grouped_files.setdefault(tournament_id, []).append((chunk, file_path))

    tasks: List[Tuple[str, List[Tuple[int, str]]]] = []
    for tournament_id, files in grouped_files.items():
        sorted_entries = [
            (chunk, str(path)) for chunk, path in sorted(files, key=lambda x: x[0])
        ]
        tasks.append((tournament_id, sorted_entries))

    worker_count = min(cpu_count(), len(tasks)) or 1
    occurrences, hands_parsed, shown_hands, total_actions = process_tournaments(
        tasks, worker_count
    )

    print("\nParsing complete!")
    print(f"  Total files processed: {len(txt_files)}")
    print(f"  Total hands parsed: {hands_parsed}")
    print(f"  Shown hands found: {shown_hands}")
    print(f"  Total actions tracked: {total_actions}")

    # Export flat fact table
    db_file = "range_analysis.duckdb"
    exporter = RangeDatabaseExporter(db_file)
    exporter.export(occurrences)

    # Generate report from DuckDB
    print("\nGenerating analysis report...")
    report_builder = RangeReportBuilder(db_file)
    report = report_builder.generate_report()
    report_file = "range_analysis_report.txt"
    with open(report_file, "w") as f:
        f.write(report)
    print(f"Report saved to {report_file}")

    # Print summary
    print("\n" + "=" * 80)
    print("QUICK SUMMARY")
    print("=" * 80)
    for (
        position,
        unique_combos,
        total_instances,
    ) in report_builder.preflop_open_summary():
        print(
            f"{position} Preflop Opening Range: {unique_combos} unique combos, {total_instances} instances"
        )

    print("\nDone! Check the output files and DuckDB warehouse for detailed analysis.")


if __name__ == "__main__":
    main()
