"""Unit tests for smurf detection scoring (runs in the Spark container)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.jobs.smurf_detection import evaluate_smurf, SMURF_PROB_THRESHOLD


def _account(age=3, games=12, rank="bronze"):
    return {"age_days": age, "games_played": games, "rank": rank}


def _combat(acc=0.62, rxn=160.0, shots=50):
    return {"avg_accuracy": acc, "avg_reaction_time": rxn, "total_shots": shots}


def test_smurf_archetype_features_flag():
    prob, reason = evaluate_smurf(_account(), _combat())
    assert prob >= SMURF_PROB_THRESHOLD, reason
    assert "bronze" in reason


def test_established_account_never_flagged():
    prob, reason = evaluate_smurf(age_and_games := _account(age=400, games=1200), _combat())
    assert prob == 0.0
    assert "established" in reason

    prob, reason = evaluate_smurf(_account(age=3, games=60), _combat())
    assert prob == 0.0, "30+ games excludes candidacy regardless of skill"


def test_new_account_with_weak_stats_not_flagged():
    prob, _ = evaluate_smurf(_account(), _combat(acc=0.19, rxn=395.0))
    assert prob < 0.30


def test_new_account_without_history_not_flagged():
    prob, reason = evaluate_smurf(_account(), None)
    assert prob == 0.0
    assert "insufficient" in reason

    prob, _ = evaluate_smurf(_account(), _combat(shots=3))
    assert prob == 0.0


def test_skill_matching_declared_rank_not_flagged():
    # Fresh diamond account performing like a diamond is legitimate
    prob, _ = evaluate_smurf(_account(rank="diamond"), _combat(acc=0.43, rxn=185.0))
    assert prob < SMURF_PROB_THRESHOLD


def test_reaction_only_superiority_can_flag():
    # Mediocre accuracy but inhuman reaction vs bronze baseline
    prob, _ = evaluate_smurf(_account(), _combat(acc=0.20, rxn=150.0))
    assert prob >= SMURF_PROB_THRESHOLD